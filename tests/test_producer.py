"""Tests for src.ingestion.producer."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.producer import fetch_ohlcv, publish_candles


class TestFetchOhlcv:
    @patch("src.ingestion.producer.ccxt")
    def test_fetch_returns_candles(self, mock_ccxt):
        mock_exchange = MagicMock()
        mock_exchange.fetch_ohlcv.return_value = [
            [1704067200000, 42000.0, 42100.0, 41900.0, 42050.0, 123.45],
            [1704067260000, 42050.0, 42150.0, 41950.0, 42100.0, 100.0],
        ]
        mock_ccxt.binance.return_value = mock_exchange

        candles = fetch_ohlcv("binance", "BTC/USDT", "1m", limit=2)

        assert len(candles) == 2
        assert candles[0]["symbol"] == "BTC/USDT"
        assert candles[0]["exchange"] == "binance"
        assert candles[0]["open"] == 42000.0
        assert candles[0]["timestamp_ms"] == 1704067200000

    @patch("src.ingestion.producer.ccxt")
    def test_fetch_includes_fetched_at(self, mock_ccxt):
        mock_exchange = MagicMock()
        mock_exchange.fetch_ohlcv.return_value = [
            [1704067200000, 42000.0, 42100.0, 41900.0, 42050.0, 123.45],
        ]
        mock_ccxt.binance.return_value = mock_exchange

        candles = fetch_ohlcv("binance", "BTC/USDT", "1m")
        assert "fetched_at" in candles[0]

    @patch("src.ingestion.producer.ccxt")
    def test_fetch_empty(self, mock_ccxt):
        mock_exchange = MagicMock()
        mock_exchange.fetch_ohlcv.return_value = []
        mock_ccxt.binance.return_value = mock_exchange

        candles = fetch_ohlcv("binance", "BTC/USDT", "1m")
        assert candles == []


class TestPublishCandles:
    def test_publish_returns_count(self, sample_candles):
        mock_producer = MagicMock()
        count = publish_candles(mock_producer, "test-topic", sample_candles)
        assert count == 5
        assert mock_producer.produce.call_count == 5
        mock_producer.flush.assert_called_once()

    def test_publish_serializes_as_json(self, sample_candle):
        mock_producer = MagicMock()
        publish_candles(mock_producer, "test-topic", [sample_candle])

        call_args = mock_producer.produce.call_args
        value = call_args[1]["value"]
        parsed = json.loads(value.decode("utf-8"))
        assert parsed["symbol"] == "BTC/USDT"
        assert parsed["open"] == 42000.0

    def test_publish_uses_symbol_timestamp_key(self, sample_candle):
        mock_producer = MagicMock()
        publish_candles(mock_producer, "test-topic", [sample_candle])

        call_args = mock_producer.produce.call_args
        key = call_args[1]["key"].decode("utf-8")
        assert "BTC/USDT" in key
        assert str(sample_candle["timestamp_ms"]) in key

    def test_publish_empty_batch(self):
        mock_producer = MagicMock()
        count = publish_candles(mock_producer, "test-topic", [])
        assert count == 0
        mock_producer.produce.assert_not_called()
