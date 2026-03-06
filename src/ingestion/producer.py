"""Kafka producer that fetches crypto OHLCV data and publishes to a topic.

Uses CCXT to fetch candles from Binance (no API key required for public data),
serializes each candle as JSON, and pushes to the raw OHLCV topic. Runs in a
continuous loop with configurable fetch intervals.
"""

from __future__ import annotations

import signal
import time
from datetime import datetime, timezone

import structlog
from confluent_kafka import KafkaError, Producer

from src.config import KafkaSettings, Settings, get_settings
from src.ingestion.serialization import create_serializer

logger = structlog.get_logger(__name__)

# Graceful shutdown
_running = True


def _signal_handler(sig: int, frame: object) -> None:
    global _running
    logger.info("shutdown_signal_received", signal=sig)
    _running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def create_producer(kafka_cfg: KafkaSettings) -> Producer:
    """Create a Confluent Kafka producer with delivery guarantees."""
    return Producer(
        {
            "bootstrap.servers": kafka_cfg.bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 1000,
            "linger.ms": 100,
            "batch.size": 16384,
            "compression.type": "snappy",
        }
    )


def _delivery_callback(err: KafkaError | None, msg: object) -> None:
    """Log delivery success or failure."""
    if err is not None:
        logger.error("delivery_failed", error=str(err))
    else:
        logger.debug("message_delivered", topic=msg.topic(), partition=msg.partition())


def fetch_ohlcv(
    exchange_id: str,
    symbol: str,
    timeframe: str,
    since: int | None = None,
    limit: int = 100,
) -> list[dict]:
    """Fetch OHLCV candles from exchange via CCXT.

    Returns a list of dicts with keys: timestamp_ms, open, high, low, close, volume.
    """
    import ccxt

    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class({"enableRateLimit": True})

    raw_candles = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)

    candles = []
    for candle in raw_candles:
        candles.append(
            {
                "symbol": symbol,
                "exchange": exchange_id,
                "timeframe": timeframe,
                "timestamp_ms": candle[0],
                "open": candle[1],
                "high": candle[2],
                "low": candle[3],
                "close": candle[4],
                "volume": candle[5],
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return candles


def publish_candles(
    producer: Producer,
    topic: str,
    candles: list[dict],
    serializer=None,
) -> int:
    """Serialize and publish candles to Kafka. Returns count of produced messages."""
    count = 0
    for candle in candles:
        key = f"{candle['symbol']}:{candle['timestamp_ms']}"
        if serializer is not None:
            key_bytes = serializer.serialize_key(key)
            value_bytes = serializer.serialize_value(candle, topic)
        else:
            import json
            key_bytes = key.encode("utf-8")
            value_bytes = json.dumps(candle).encode("utf-8")
        producer.produce(
            topic,
            key=key_bytes,
            value=value_bytes,
            callback=_delivery_callback,
        )
        count += 1

    producer.flush(timeout=10)
    return count


def run(settings: Settings | None = None) -> None:
    """Main producer loop: fetch candles and publish to Kafka."""
    if settings is None:
        settings = get_settings()

    kafka_cfg = settings.kafka
    data_cfg = settings.data_source

    producer = create_producer(kafka_cfg)
    serializer = create_serializer(settings.schema_registry)
    last_timestamp: int | None = None

    logger.info(
        "producer_started",
        symbol=data_cfg.symbol,
        exchange=data_cfg.exchange,
        topic=kafka_cfg.topic_raw_ohlcv,
    )

    while _running:
        try:
            candles = fetch_ohlcv(
                exchange_id=data_cfg.exchange,
                symbol=data_cfg.symbol,
                timeframe=data_cfg.timeframe,
                since=last_timestamp,
                limit=100,
            )

            if candles:
                # Deduplicate: only publish candles newer than last seen
                if last_timestamp is not None:
                    candles = [c for c in candles if c["timestamp_ms"] > last_timestamp]

                if candles:
                    count = publish_candles(producer, kafka_cfg.topic_raw_ohlcv, candles, serializer)
                    last_timestamp = max(c["timestamp_ms"] for c in candles)
                    logger.info(
                        "batch_published",
                        count=count,
                        latest_ts=datetime.fromtimestamp(
                            last_timestamp / 1000, tz=timezone.utc
                        ).isoformat(),
                    )

        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception("fetch_publish_error")

        # Wait before next fetch
        for _ in range(data_cfg.fetch_interval_seconds):
            if not _running:
                break
            time.sleep(1)

    producer.flush(timeout=30)
    logger.info("producer_stopped")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    run()
