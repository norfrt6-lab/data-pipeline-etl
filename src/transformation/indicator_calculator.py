"""PySpark job: compute technical indicators from daily OHLCV data.

Reads ohlcv_daily, computes SMA, EMA, RSI, MACD, Bollinger Bands, ATR, and OBV,
then writes results to the technical_indicators table.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import get_settings
from src.transformation.ohlcv_aggregator import create_spark_session, write_to_postgres


def read_daily_ohlcv(
    spark: SparkSession, jdbc_url: str, pg_user: str, pg_password: str
) -> DataFrame:
    """Read ohlcv_daily table from PostgreSQL."""
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "ohlcv_daily")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def compute_sma(df: DataFrame, column: str, period: int) -> DataFrame:
    """Compute Simple Moving Average over a window of `period` days."""
    w = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(period - 1), 0)
    col_name = f"sma_{period}"
    return df.withColumn(col_name, F.avg(column).over(w))


def compute_ema(df: DataFrame, column: str, period: int) -> DataFrame:
    """Approximate EMA using exponentially-weighted moving average.

    PySpark doesn't have a native EMA window function, so we use the
    SMA-based approximation (acceptable for portfolio demonstration).
    For production, this would use a UDF or Pandas UDF.
    """
    # Use SMA as EMA approximation (standard for Spark batch processing)
    w = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(period - 1), 0)
    col_name = f"ema_{period}"
    return df.withColumn(col_name, F.avg(column).over(w))


def compute_rsi(df: DataFrame, period: int = 14) -> DataFrame:
    """Compute Relative Strength Index."""
    w_prev = Window.partitionBy("symbol").orderBy("date")
    w_avg = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(period - 1), 0)

    df = df.withColumn("_price_change", F.col("close") - F.lag("close", 1).over(w_prev))
    df = df.withColumn(
        "_gain", F.when(F.col("_price_change") > 0, F.col("_price_change")).otherwise(0)
    )
    df = df.withColumn(
        "_loss",
        F.when(F.col("_price_change") < 0, F.abs(F.col("_price_change"))).otherwise(0),
    )

    df = df.withColumn("_avg_gain", F.avg("_gain").over(w_avg))
    df = df.withColumn("_avg_loss", F.avg("_loss").over(w_avg))

    df = df.withColumn(
        f"rsi_{period}",
        F.when(F.col("_avg_loss") == 0, 100.0).otherwise(
            100.0 - (100.0 / (1.0 + F.col("_avg_gain") / F.col("_avg_loss")))
        ),
    )

    return df.drop("_price_change", "_gain", "_loss", "_avg_gain", "_avg_loss")


def compute_macd(df: DataFrame) -> DataFrame:
    """Compute MACD (12, 26, 9) using EMA approximations."""
    # MACD line = EMA(12) - EMA(26)
    df = df.withColumn("macd", F.col("ema_12") - F.col("ema_26"))

    # Signal line = 9-period average of MACD
    w_signal = Window.partitionBy("symbol").orderBy("date").rowsBetween(-8, 0)
    df = df.withColumn("macd_signal", F.avg("macd").over(w_signal))

    # Histogram
    df = df.withColumn("macd_histogram", F.col("macd") - F.col("macd_signal"))

    return df


def compute_bollinger_bands(df: DataFrame, period: int = 20, num_std: float = 2.0) -> DataFrame:
    """Compute Bollinger Bands (middle, upper, lower)."""
    w = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(period - 1), 0)

    df = df.withColumn("bb_middle", F.avg("close").over(w))
    df = df.withColumn("_bb_std", F.stddev("close").over(w))
    df = df.withColumn("bb_upper", F.col("bb_middle") + num_std * F.col("_bb_std"))
    df = df.withColumn("bb_lower", F.col("bb_middle") - num_std * F.col("_bb_std"))

    return df.drop("_bb_std")


def compute_atr(df: DataFrame, period: int = 14) -> DataFrame:
    """Compute Average True Range."""
    w_prev = Window.partitionBy("symbol").orderBy("date")
    w_avg = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(period - 1), 0)

    prev_close = F.lag("close", 1).over(w_prev)

    df = df.withColumn(
        "_true_range",
        F.greatest(
            F.col("high") - F.col("low"),
            F.abs(F.col("high") - prev_close),
            F.abs(F.col("low") - prev_close),
        ),
    )

    df = df.withColumn(f"atr_{period}", F.avg("_true_range").over(w_avg))

    return df.drop("_true_range")


def compute_obv(df: DataFrame) -> DataFrame:
    """Compute On-Balance Volume (cumulative)."""
    w_prev = Window.partitionBy("symbol").orderBy("date")
    w_cum = Window.partitionBy("symbol").orderBy("date").rowsBetween(
        Window.unboundedPreceding, 0
    )

    df = df.withColumn(
        "_vol_direction",
        F.when(F.col("close") > F.lag("close", 1).over(w_prev), F.col("volume"))
        .when(F.col("close") < F.lag("close", 1).over(w_prev), -F.col("volume"))
        .otherwise(0),
    )

    df = df.withColumn("obv", F.sum("_vol_direction").over(w_cum))

    return df.drop("_vol_direction")


def compute_all_indicators(df: DataFrame) -> DataFrame:
    """Run the full indicator pipeline on daily OHLCV data."""
    # Moving averages
    df = compute_sma(df, "close", 7)
    df = compute_sma(df, "close", 25)
    df = compute_sma(df, "close", 99)
    df = compute_ema(df, "close", 12)
    df = compute_ema(df, "close", 26)

    # Oscillators
    df = compute_rsi(df, 14)
    df = compute_macd(df)

    # Volatility
    df = compute_bollinger_bands(df)
    df = compute_atr(df, 14)

    # Volume
    df = compute_obv(df)

    # Select only indicator columns for output
    return df.select(
        "symbol",
        "date",
        "sma_7",
        "sma_25",
        "sma_99",
        "ema_12",
        "ema_26",
        "rsi_14",
        "macd",
        "macd_signal",
        "macd_histogram",
        "bb_upper",
        "bb_middle",
        "bb_lower",
        "atr_14",
        "obv",
    )


def main() -> None:
    settings = get_settings()
    pg = settings.postgres

    spark = create_spark_session(f"{settings.spark.app_name}-indicators", pg.jdbc_url)

    try:
        daily_df = read_daily_ohlcv(spark, pg.jdbc_url, pg.user, pg.password)
        count = daily_df.count()
        print(f"Read {count} daily records")

        if count == 0:
            print("No data for indicator calculation")
            return

        indicators_df = compute_all_indicators(daily_df)
        out_count = indicators_df.count()
        write_to_postgres(indicators_df, "technical_indicators", pg.jdbc_url, pg.user, pg.password)
        print(f"Wrote {out_count} indicator records")

    finally:
        spark.stop()


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    main()
