"""PySpark job: aggregate raw minute-level OHLCV data into hourly and daily candles.

Reads from the raw_ohlcv staging table, computes proper OHLCV aggregations
(first open, max high, min low, last close, sum volume), and writes to
ohlcv_hourly and ohlcv_daily warehouse tables.
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import get_settings


def create_spark_session(app_name: str, pg_jdbc_url: str) -> SparkSession:
    """Create a SparkSession configured for PostgreSQL access."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_raw_ohlcv(spark: SparkSession, jdbc_url: str, pg_user: str, pg_password: str) -> DataFrame:
    """Read raw_ohlcv table from PostgreSQL."""
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "raw_ohlcv")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def aggregate_hourly(df: DataFrame) -> DataFrame:
    """Aggregate minute-level data into hourly candles.

    Uses window functions to correctly identify the first and last records
    within each hour bucket for open/close prices.
    """
    # Add hour bucket column
    df_with_hour = df.withColumn(
        "hour_start",
        F.date_trunc("hour", (F.col("timestamp_ms") / 1000).cast("timestamp")),
    )

    # Window for first/last within each hour
    w_asc = Window.partitionBy("symbol", "exchange", "hour_start").orderBy("timestamp_ms")
    w_desc = Window.partitionBy("symbol", "exchange", "hour_start").orderBy(
        F.desc("timestamp_ms")
    )

    df_ranked = df_with_hour.withColumn("rn_first", F.row_number().over(w_asc)).withColumn(
        "rn_last", F.row_number().over(w_desc)
    )

    # Get open (first) and close (last) prices
    opens = df_ranked.filter(F.col("rn_first") == 1).select(
        "symbol", "exchange", "hour_start", F.col("open").alias("open_price")
    )
    closes = df_ranked.filter(F.col("rn_last") == 1).select(
        "symbol", "exchange", "hour_start", F.col("close").alias("close_price")
    )

    # Aggregate high, low, volume, count
    agg = (
        df_with_hour.groupBy("symbol", "exchange", "hour_start")
        .agg(
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.sum("volume").alias("volume"),
            F.count("*").alias("trade_count"),
            F.sum(F.col("close") * F.col("volume")).alias("_vwap_num"),
            F.sum("volume").alias("_vwap_den"),
        )
        .withColumn(
            "vwap",
            F.when(F.col("_vwap_den") > 0, F.col("_vwap_num") / F.col("_vwap_den")).otherwise(
                None
            ),
        )
        .drop("_vwap_num", "_vwap_den")
    )

    # Join open and close
    result = (
        agg.join(opens, on=["symbol", "exchange", "hour_start"])
        .join(closes, on=["symbol", "exchange", "hour_start"])
        .select(
            "symbol",
            "exchange",
            "hour_start",
            F.col("open_price").alias("open"),
            "high",
            "low",
            F.col("close_price").alias("close"),
            "volume",
            "trade_count",
            "vwap",
        )
    )

    return result


def aggregate_daily(df: DataFrame) -> DataFrame:
    """Aggregate minute-level data into daily candles."""
    df_with_date = df.withColumn(
        "date",
        F.to_date((F.col("timestamp_ms") / 1000).cast("timestamp")),
    )

    w_asc = Window.partitionBy("symbol", "exchange", "date").orderBy("timestamp_ms")
    w_desc = Window.partitionBy("symbol", "exchange", "date").orderBy(F.desc("timestamp_ms"))

    df_ranked = df_with_date.withColumn("rn_first", F.row_number().over(w_asc)).withColumn(
        "rn_last", F.row_number().over(w_desc)
    )

    opens = df_ranked.filter(F.col("rn_first") == 1).select(
        "symbol", "exchange", "date", F.col("open").alias("open_price")
    )
    closes = df_ranked.filter(F.col("rn_last") == 1).select(
        "symbol", "exchange", "date", F.col("close").alias("close_price")
    )

    agg = (
        df_with_date.groupBy("symbol", "exchange", "date")
        .agg(
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.sum("volume").alias("volume"),
            F.count("*").alias("trade_count"),
            F.sum(F.col("close") * F.col("volume")).alias("_vwap_num"),
            F.sum("volume").alias("_vwap_den"),
        )
        .withColumn(
            "vwap",
            F.when(F.col("_vwap_den") > 0, F.col("_vwap_num") / F.col("_vwap_den")).otherwise(
                None
            ),
        )
        .drop("_vwap_num", "_vwap_den")
    )

    result = (
        agg.join(opens, on=["symbol", "exchange", "date"])
        .join(closes, on=["symbol", "exchange", "date"])
        .select(
            "symbol",
            "exchange",
            "date",
            F.col("open_price").alias("open"),
            "high",
            "low",
            F.col("close_price").alias("close"),
            "volume",
            "trade_count",
            "vwap",
        )
    )

    return result


def write_to_postgres(
    df: DataFrame,
    table: str,
    jdbc_url: str,
    pg_user: str,
    pg_password: str,
    mode: str = "append",
) -> None:
    """Write a DataFrame to a PostgreSQL table."""
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )


def main() -> None:
    settings = get_settings()
    pg = settings.postgres

    spark = create_spark_session(f"{settings.spark.app_name}-aggregator", pg.jdbc_url)

    try:
        raw_df = read_raw_ohlcv(spark, pg.jdbc_url, pg.user, pg.password)
        raw_count = raw_df.count()
        print(f"Read {raw_count} raw records")

        if raw_count == 0:
            print("No data to aggregate")
            return

        # Hourly aggregation
        hourly_df = aggregate_hourly(raw_df)
        hourly_count = hourly_df.count()
        write_to_postgres(hourly_df, "ohlcv_hourly", pg.jdbc_url, pg.user, pg.password)
        print(f"Wrote {hourly_count} hourly candles")

        # Daily aggregation
        daily_df = aggregate_daily(raw_df)
        daily_count = daily_df.count()
        write_to_postgres(daily_df, "ohlcv_daily", pg.jdbc_url, pg.user, pg.password)
        print(f"Wrote {daily_count} daily candles")

    finally:
        spark.stop()


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    main()
