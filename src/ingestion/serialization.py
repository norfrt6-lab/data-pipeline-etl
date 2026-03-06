"""Avro serialization/deserialization for Kafka messages.

Uses Confluent Schema Registry for schema evolution and compatibility checks.
Falls back to JSON serialization when schema registry is unavailable.
"""

from __future__ import annotations

import json

import structlog
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
    StringSerializer,
)

from src.config import SchemaRegistrySettings

logger = structlog.get_logger(__name__)

OHLCV_SCHEMA = """
{
  "type": "record",
  "name": "OhlcvCandle",
  "namespace": "com.crypto.pipeline",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "exchange", "type": "string"},
    {"name": "timeframe", "type": "string"},
    {"name": "timestamp_ms", "type": "long"},
    {"name": "open", "type": "double"},
    {"name": "high", "type": "double"},
    {"name": "low", "type": "double"},
    {"name": "close", "type": "double"},
    {"name": "volume", "type": "double"},
    {"name": "fetched_at", "type": "string"}
  ]
}
"""


def _candle_to_dict(candle: dict, ctx: SerializationContext) -> dict:
    """Convert candle dict to Avro-compatible dict."""
    return {
        "symbol": candle["symbol"],
        "exchange": candle["exchange"],
        "timeframe": candle.get("timeframe", "1m"),
        "timestamp_ms": int(candle["timestamp_ms"]),
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
        "volume": float(candle["volume"]),
        "fetched_at": candle.get("fetched_at", ""),
    }


def _dict_to_candle(data: dict, ctx: SerializationContext) -> dict:
    """Convert deserialized Avro record back to candle dict."""
    return data


class AvroOhlcvSerializer:
    """Avro serializer with Schema Registry integration."""

    def __init__(self, schema_registry_cfg: SchemaRegistrySettings) -> None:
        self._registry_client = SchemaRegistryClient({"url": schema_registry_cfg.url})
        self._avro_serializer = AvroSerializer(
            self._registry_client,
            OHLCV_SCHEMA,
            to_dict=_candle_to_dict,
            conf={"auto.register.schemas": True},
        )
        self._key_serializer = StringSerializer("utf_8")
        logger.info("avro_serializer_initialized", registry_url=schema_registry_cfg.url)

    def serialize_key(self, key: str) -> bytes:
        result: bytes = self._key_serializer(key)
        assert result is not None
        return result

    def serialize_value(self, candle: dict, topic: str) -> bytes:
        ctx = SerializationContext(topic, MessageField.VALUE)
        result: bytes = self._avro_serializer(candle, ctx)
        return result


class AvroOhlcvDeserializer:
    """Avro deserializer with Schema Registry integration."""

    def __init__(self, schema_registry_cfg: SchemaRegistrySettings) -> None:
        self._registry_client = SchemaRegistryClient({"url": schema_registry_cfg.url})
        self._avro_deserializer = AvroDeserializer(
            self._registry_client,
            OHLCV_SCHEMA,
            from_dict=_dict_to_candle,
        )
        self._key_deserializer = StringDeserializer("utf_8")
        logger.info("avro_deserializer_initialized", registry_url=schema_registry_cfg.url)

    def deserialize_key(self, data: bytes) -> str | None:
        if data is None:
            return None
        result: str = self._key_deserializer(data)
        return result

    def deserialize_value(self, data: bytes, topic: str) -> dict | None:
        if data is None:
            return None
        ctx = SerializationContext(topic, MessageField.VALUE)
        result: dict | None = self._avro_deserializer(data, ctx)
        return result


class JsonFallbackSerializer:
    """JSON serializer used when Schema Registry is unavailable."""

    def serialize_key(self, key: str) -> bytes:
        return key.encode("utf-8")

    def serialize_value(self, candle: dict, topic: str) -> bytes:
        return json.dumps(candle).encode("utf-8")


class JsonFallbackDeserializer:
    """JSON deserializer used when Schema Registry is unavailable."""

    def deserialize_key(self, data: bytes) -> str | None:
        if data is None:
            return None
        return data.decode("utf-8")

    def deserialize_value(self, data: bytes, topic: str) -> dict | None:
        if data is None:
            return None
        try:
            result: dict = json.loads(data.decode("utf-8"))
            return result
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None


def create_serializer(
    schema_registry_cfg: SchemaRegistrySettings,
) -> AvroOhlcvSerializer | JsonFallbackSerializer:
    """Create serializer — Avro if registry available, JSON fallback otherwise."""
    if schema_registry_cfg.url:
        try:
            return AvroOhlcvSerializer(schema_registry_cfg)
        except Exception as exc:
            logger.warning("schema_registry_unavailable", error=str(exc))

    logger.info("using_json_fallback_serializer")
    return JsonFallbackSerializer()


def create_deserializer(
    schema_registry_cfg: SchemaRegistrySettings,
) -> AvroOhlcvDeserializer | JsonFallbackDeserializer:
    """Create deserializer — Avro if registry available, JSON fallback otherwise."""
    if schema_registry_cfg.url:
        try:
            return AvroOhlcvDeserializer(schema_registry_cfg)
        except Exception as exc:
            logger.warning("schema_registry_unavailable", error=str(exc))

    logger.info("using_json_fallback_deserializer")
    return JsonFallbackDeserializer()
