"""Tests for src.ingestion.serialization."""

from __future__ import annotations

import json

from src.config import SchemaRegistrySettings
from src.ingestion.serialization import (
    JsonFallbackDeserializer,
    JsonFallbackSerializer,
    _candle_to_dict,
    _dict_to_candle,
    create_deserializer,
    create_serializer,
)


class TestJsonFallbackSerializer:
    def test_serialize_key(self):
        s = JsonFallbackSerializer()
        result = s.serialize_key("BTC/USDT:123")
        assert result == b"BTC/USDT:123"

    def test_serialize_value(self, sample_candle):
        s = JsonFallbackSerializer()
        result = s.serialize_value(sample_candle, "test-topic")
        parsed = json.loads(result.decode("utf-8"))
        assert parsed["symbol"] == "BTC/USDT"
        assert parsed["open"] == 42000.0

    def test_roundtrip(self, sample_candle):
        s = JsonFallbackSerializer()
        d = JsonFallbackDeserializer()
        topic = "test-topic"
        raw = s.serialize_value(sample_candle, topic)
        result = d.deserialize_value(raw, topic)
        assert result["symbol"] == sample_candle["symbol"]
        assert result["close"] == sample_candle["close"]


class TestJsonFallbackDeserializer:
    def test_deserialize_key(self):
        d = JsonFallbackDeserializer()
        assert d.deserialize_key(b"BTC/USDT:123") == "BTC/USDT:123"

    def test_deserialize_key_none(self):
        d = JsonFallbackDeserializer()
        assert d.deserialize_key(None) is None

    def test_deserialize_value(self):
        d = JsonFallbackDeserializer()
        raw = json.dumps({"symbol": "ETH/USDT", "close": 2500.0}).encode("utf-8")
        result = d.deserialize_value(raw, "test-topic")
        assert result["symbol"] == "ETH/USDT"

    def test_deserialize_value_none(self):
        d = JsonFallbackDeserializer()
        assert d.deserialize_value(None, "test-topic") is None

    def test_deserialize_value_invalid_json(self):
        d = JsonFallbackDeserializer()
        assert d.deserialize_value(b"not json", "test-topic") is None

    def test_deserialize_value_invalid_utf8(self):
        d = JsonFallbackDeserializer()
        assert d.deserialize_value(b"\xff\xfe", "test-topic") is None


class TestCandleToDictHelper:
    def test_converts_types(self):
        candle = {
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "timeframe": "1m",
            "timestamp_ms": "1704067200000",
            "open": "42000",
            "high": "42100",
            "low": "41900",
            "close": "42050",
            "volume": "123.45",
            "fetched_at": "2024-01-01T00:00:00Z",
        }
        result = _candle_to_dict(candle, None)
        assert isinstance(result["timestamp_ms"], int)
        assert isinstance(result["open"], float)
        assert result["timestamp_ms"] == 1704067200000

    def test_default_timeframe(self):
        candle = {
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "timestamp_ms": 0,
            "open": 0,
            "high": 0,
            "low": 0,
            "close": 0,
            "volume": 0,
        }
        result = _candle_to_dict(candle, None)
        assert result["timeframe"] == "1m"
        assert result["fetched_at"] == ""

    def test_dict_to_candle_passthrough(self):
        data = {"symbol": "BTC/USDT", "close": 42000.0}
        assert _dict_to_candle(data, None) is data


class TestCreateSerializerFactory:
    def test_falls_back_to_json_when_registry_unreachable(self):
        cfg = SchemaRegistrySettings(url="http://localhost:99999")
        s = create_serializer(cfg)
        assert isinstance(s, JsonFallbackSerializer)

    def test_falls_back_to_json_when_url_empty(self):
        cfg = SchemaRegistrySettings(url="")
        s = create_serializer(cfg)
        assert isinstance(s, JsonFallbackSerializer)


class TestCreateDeserializerFactory:
    def test_falls_back_to_json_when_registry_unreachable(self):
        cfg = SchemaRegistrySettings(url="http://localhost:99999")
        d = create_deserializer(cfg)
        assert isinstance(d, JsonFallbackDeserializer)

    def test_falls_back_to_json_when_url_empty(self):
        cfg = SchemaRegistrySettings(url="")
        d = create_deserializer(cfg)
        assert isinstance(d, JsonFallbackDeserializer)
