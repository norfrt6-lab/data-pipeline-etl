"""Tests for src.ingestion.producer."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.ingestion.producer import _delivery_callback, fetch_ohlcv, publish_candles


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

    @patch("src.ingestion.producer.ccxt")
    def test_fetch_passes_since_parameter(self, mock_ccxt):
        mock_exchange = MagicMock()
        mock_exchange.fetch_ohlcv.return_value = []
        mock_ccxt.binance.return_value = mock_exchange

        fetch_ohlcv("binance", "BTC/USDT", "1m", since=1704067200000)
        mock_exchange.fetch_ohlcv.assert_called_once_with(
            "BTC/USDT", "1m", since=1704067200000, limit=100
        )


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

    def test_publish_with_serializer(self, sample_candle):
        mock_producer = MagicMock()
        mock_serializer = MagicMock()
        mock_serializer.serialize_key.return_value = b"key-bytes"
        mock_serializer.serialize_value.return_value = b"value-bytes"

        count = publish_candles(
            mock_producer, "test-topic", [sample_candle], serializer=mock_serializer
        )
        assert count == 1
        mock_serializer.serialize_key.assert_called_once()
        mock_serializer.serialize_value.assert_called_once_with(sample_candle, "test-topic")
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["key"] == b"key-bytes"
        assert call_kwargs["value"] == b"value-bytes"


class TestDeliveryCallback:
    def test_success_no_error(self):
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test-topic"
        mock_msg.partition.return_value = 0
        # Should not raise
        _delivery_callback(None, mock_msg)

    def test_error_logged(self):
        mock_msg = MagicMock()
        mock_err = MagicMock()
        mock_err.__str__ = MagicMock(return_value="delivery failed")
        # Should not raise
        _delivery_callback(mock_err, mock_msg)
