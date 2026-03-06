"""Shared test fixtures for the data pipeline test suite."""

from __future__ import annotations

import pytest


@pytest.fixture
def sample_candle() -> dict:
    """A single valid OHLCV candle record."""
    return {
        "symbol": "BTC/USDT",
        "exchange": "binance",
        "timeframe": "1m",
        "timestamp_ms": 1704067200000,  # 2024-01-01 00:00:00 UTC
        "open": 42000.0,
        "high": 42100.0,
        "low": 41900.0,
        "close": 42050.0,
        "volume": 123.45,
        "fetched_at": "2024-01-01T00:01:00+00:00",
    }


@pytest.fixture
def sample_candles() -> list[dict]:
    """A batch of valid OHLCV candle records spanning 5 minutes."""
    base_ts = 1704067200000
    candles = []
    for i in range(5):
        candles.append(
            {
                "symbol": "BTC/USDT",
                "exchange": "binance",
                "timeframe": "1m",
                "timestamp_ms": base_ts + i * 60000,
                "open": 42000.0 + i * 10,
                "high": 42100.0 + i * 10,
                "low": 41900.0 + i * 10,
                "close": 42050.0 + i * 10,
                "volume": 100.0 + i * 5,
                "fetched_at": "2024-01-01T00:01:00+00:00",
            }
        )
    return candles


@pytest.fixture
def sample_raw_value() -> bytes:
    """A valid Kafka message value as bytes."""
    import json

    return json.dumps(
        {
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "timestamp_ms": 1704067200000,
            "open": 42000.0,
            "high": 42100.0,
            "low": 41900.0,
            "close": 42050.0,
            "volume": 123.45,
        }
    ).encode("utf-8")
