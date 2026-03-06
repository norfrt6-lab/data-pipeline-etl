"""Tests for src.ingestion.consumer."""

from __future__ import annotations

import json

from src.ingestion.consumer import parse_message


class TestParseMessage:
    def test_valid_message(self, sample_raw_value):
        result = parse_message(sample_raw_value)
        assert result is not None
        assert result["symbol"] == "BTC/USDT"
        assert result["open"] == 42000.0

    def test_invalid_json(self):
        result = parse_message(b"not json")
        assert result is None

    def test_missing_fields(self):
        incomplete = json.dumps({"symbol": "BTC/USDT", "open": 42000.0}).encode("utf-8")
        result = parse_message(incomplete)
        assert result is None

    def test_all_required_fields_present(self):
        full = json.dumps(
            {
                "symbol": "ETH/USDT",
                "exchange": "binance",
                "timestamp_ms": 1704067200000,
                "open": 2300.0,
                "high": 2310.0,
                "low": 2290.0,
                "close": 2305.0,
                "volume": 5000.0,
            }
        ).encode("utf-8")
        result = parse_message(full)
        assert result is not None
        assert result["symbol"] == "ETH/USDT"

    def test_extra_fields_preserved(self):
        data = {
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "timestamp_ms": 1704067200000,
            "open": 42000.0,
            "high": 42100.0,
            "low": 41900.0,
            "close": 42050.0,
            "volume": 123.45,
            "extra_field": "should be kept",
        }
        result = parse_message(json.dumps(data).encode("utf-8"))
        assert result is not None
        assert result["extra_field"] == "should be kept"

    def test_unicode_error(self):
        result = parse_message(b"\xff\xfe")
        assert result is None
