"""Tests for src.transformation.ohlcv_aggregator using local Spark."""

from __future__ import annotations

import pytest

# Only run if pyspark is available
pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from src.transformation.ohlcv_aggregator import aggregate_daily, aggregate_hourly  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-aggregator")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def raw_df(spark):
    """Create a test DataFrame with minute-level OHLCV data."""
    # 3 candles: 2 in the same hour, 1 in the next hour
    data = [
        ("BTC/USDT", "binance", 1704067200000, 42000.0, 42100.0, 41900.0, 42050.0, 100.0),
        ("BTC/USDT", "binance", 1704067260000, 42050.0, 42200.0, 41950.0, 42100.0, 150.0),
        ("BTC/USDT", "binance", 1704070800000, 42100.0, 42300.0, 42000.0, 42250.0, 200.0),
    ]
    columns = ["symbol", "exchange", "timestamp_ms", "open", "high", "low", "close", "volume"]
    return spark.createDataFrame(data, columns)


class TestAggregateHourly:
    def test_output_has_expected_columns(self, raw_df):
        result = aggregate_hourly(raw_df)
        expected_cols = {
            "symbol",
            "exchange",
            "hour_start",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_groups_by_hour(self, raw_df):
        result = aggregate_hourly(raw_df)
        assert result.count() == 2  # 2 distinct hours

    def test_high_is_max(self, raw_df):
        result = aggregate_hourly(raw_df)
        rows = result.orderBy("hour_start").collect()
        # First hour: max of 42100 and 42200
        assert rows[0]["high"] == 42200.0

    def test_volume_is_sum(self, raw_df):
        result = aggregate_hourly(raw_df)
        rows = result.orderBy("hour_start").collect()
        # First hour: 100 + 150
        assert rows[0]["volume"] == 250.0


class TestAggregateDaily:
    def test_groups_by_date(self, raw_df):
        result = aggregate_daily(raw_df)
        # All 3 candles are on the same day (2024-01-01)
        assert result.count() == 1

    def test_volume_is_total(self, raw_df):
        result = aggregate_daily(raw_df)
        row = result.collect()[0]
        assert row["volume"] == 450.0  # 100 + 150 + 200

    def test_open_is_first_close_is_last(self, raw_df):
        result = aggregate_daily(raw_df)
        row = result.collect()[0]
        assert row["open"] == 42000.0  # First candle's open
        assert row["close"] == 42250.0  # Last candle's close
