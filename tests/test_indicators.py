"""Tests for src.transformation.indicator_calculator using local Spark."""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from src.transformation.indicator_calculator import (  # noqa: E402
    compute_all_indicators,
    compute_atr,
    compute_bollinger_bands,
    compute_obv,
    compute_rsi,
    compute_sma,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-indicators")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def daily_df(spark):
    """Create a test DataFrame with 30 days of daily OHLCV data."""
    from datetime import date, timedelta

    data = []
    base_price = 42000.0
    for i in range(30):
        d = date(2024, 1, 1) + timedelta(days=i)
        price = base_price + i * 100  # Steady uptrend
        data.append((
            "BTC/USDT",
            "binance",
            d,
            price,
            price + 200,
            price - 100,
            price + 50,
            1000.0 + i * 10,
            1,
            price + 25,
        ))

    columns = ["symbol", "exchange", "date", "open", "high", "low", "close", "volume", "trade_count", "vwap"]
    return spark.createDataFrame(data, columns)


class TestComputeSma:
    def test_sma_column_added(self, daily_df):
        result = compute_sma(daily_df, "close", 7)
        assert "sma_7" in result.columns

    def test_sma_value_is_average(self, daily_df):
        result = compute_sma(daily_df, "close", 7)
        rows = result.orderBy("date").collect()
        # After 7 days, SMA should be the average of the last 7 closes
        sma_val = rows[6]["sma_7"]
        expected = sum(42050.0 + i * 100 for i in range(7)) / 7
        assert abs(sma_val - expected) < 0.01


class TestComputeRsi:
    def test_rsi_column_added(self, daily_df):
        result = compute_rsi(daily_df, 14)
        assert "rsi_14" in result.columns

    def test_rsi_bounded(self, daily_df):
        result = compute_rsi(daily_df, 14)
        rows = result.collect()
        for row in rows:
            if row["rsi_14"] is not None:
                assert 0 <= row["rsi_14"] <= 100


class TestComputeBollingerBands:
    def test_three_bands_added(self, daily_df):
        result = compute_bollinger_bands(daily_df)
        assert "bb_upper" in result.columns
        assert "bb_middle" in result.columns
        assert "bb_lower" in result.columns

    def test_upper_above_lower(self, daily_df):
        result = compute_bollinger_bands(daily_df)
        rows = result.collect()
        for row in rows:
            if row["bb_upper"] is not None and row["bb_lower"] is not None:
                assert row["bb_upper"] >= row["bb_lower"]


class TestComputeAtr:
    def test_atr_column_added(self, daily_df):
        result = compute_atr(daily_df, 14)
        assert "atr_14" in result.columns

    def test_atr_positive(self, daily_df):
        result = compute_atr(daily_df, 14)
        rows = result.collect()
        for row in rows:
            if row["atr_14"] is not None:
                assert row["atr_14"] >= 0


class TestComputeObv:
    def test_obv_column_added(self, daily_df):
        result = compute_obv(daily_df)
        assert "obv" in result.columns


class TestComputeAllIndicators:
    def test_all_columns_present(self, daily_df):
        result = compute_all_indicators(daily_df)
        expected = {
            "symbol", "date", "sma_7", "sma_25", "sma_99",
            "ema_12", "ema_26", "rsi_14", "macd", "macd_signal",
            "macd_histogram", "bb_upper", "bb_middle", "bb_lower",
            "atr_14", "obv",
        }
        assert expected.issubset(set(result.columns))

    def test_row_count_preserved(self, daily_df):
        result = compute_all_indicators(daily_df)
        assert result.count() == 30
