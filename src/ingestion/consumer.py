"""Kafka consumer that reads raw OHLCV candles and writes to PostgreSQL staging.

Consumes messages from the raw OHLCV topic, deserializes JSON, and batch-inserts
into the raw_ohlcv table using COPY for efficiency. Failed messages are routed
to a dead letter queue topic.
"""

from __future__ import annotations

import signal
import time

import psycopg2
import psycopg2.extras
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from src.config import KafkaSettings, PostgresSettings, Settings, get_settings
from src.ingestion.serialization import create_deserializer

logger = structlog.get_logger(__name__)

_running = True


def _signal_handler(sig: int, frame: object) -> None:
    global _running
    logger.info("shutdown_signal_received", signal=sig)
    _running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def create_consumer(kafka_cfg: KafkaSettings) -> Consumer:
    """Create a Confluent Kafka consumer with at-least-once semantics."""
    return Consumer(
        {
            "bootstrap.servers": kafka_cfg.bootstrap_servers,
            "group.id": kafka_cfg.consumer_group,
            "auto.offset.reset": kafka_cfg.auto_offset_reset,
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
        }
    )


def create_dlq_producer(kafka_cfg: KafkaSettings) -> Producer:
    """Create a producer for the dead letter queue."""
    return Producer({"bootstrap.servers": kafka_cfg.bootstrap_servers})


def connect_postgres(pg_cfg: PostgresSettings) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL with retry logic."""
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=pg_cfg.host,
                port=pg_cfg.port,
                dbname=pg_cfg.db,
                user=pg_cfg.user,
                password=pg_cfg.password,
            )
            conn.autocommit = False
            logger.info("postgres_connected", host=pg_cfg.host, db=pg_cfg.db)
            return conn
        except psycopg2.OperationalError:
            if attempt == max_retries:
                raise
            logger.warning("postgres_retry", attempt=attempt, max=max_retries)
            time.sleep(2**attempt)

    raise RuntimeError("Failed to connect to PostgreSQL")


def insert_batch(
    conn: psycopg2.extensions.connection,
    records: list[dict],
) -> int:
    """Batch upsert records into raw_ohlcv. Returns count of inserted rows."""
    if not records:
        return 0

    sql = """
        INSERT INTO raw_ohlcv (symbol, exchange, timestamp_ms, open, high, low, close, volume)
        VALUES (%(symbol)s, %(exchange)s, %(timestamp_ms)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
        ON CONFLICT (symbol, exchange, timestamp_ms) DO NOTHING
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=500)

    conn.commit()
    return len(records)


def parse_message(raw_value: bytes, deserializer: object = None, topic: str = "") -> dict | None:
    """Deserialize a Kafka message value. Returns None on parse failure."""
    try:
        if deserializer is not None:
            data = deserializer.deserialize_value(raw_value, topic)
        else:
            import json

            data = json.loads(raw_value.decode("utf-8"))

        if data is None:
            return None

        # Validate required fields
        required = {"symbol", "exchange", "timestamp_ms", "open", "high", "low", "close", "volume"}
        if not required.issubset(data.keys()):
            logger.warning("missing_fields", keys=list(data.keys()))
            return None
        result: dict = data
        return result
    except Exception as exc:
        logger.warning("parse_error", error=str(exc))
        return None


def send_to_dlq(dlq_producer: Producer, topic: str, raw_value: bytes, reason: str) -> None:
    """Send a failed message to the dead letter queue."""
    headers: list[tuple[str, str | bytes | None]] = [("error_reason", reason.encode("utf-8"))]
    dlq_producer.produce(topic, value=raw_value, headers=headers)
    dlq_producer.poll(0)


def run(settings: Settings | None = None) -> None:
    """Main consumer loop: poll Kafka, batch insert to PostgreSQL."""
    if settings is None:
        settings = get_settings()

    kafka_cfg = settings.kafka
    pg_cfg = settings.postgres

    consumer = create_consumer(kafka_cfg)
    dlq_producer = create_dlq_producer(kafka_cfg)
    conn = connect_postgres(pg_cfg)
    deserializer = create_deserializer(settings.schema_registry)

    consumer.subscribe([kafka_cfg.topic_raw_ohlcv])
    logger.info("consumer_started", topic=kafka_cfg.topic_raw_ohlcv, group=kafka_cfg.consumer_group)

    batch: list[dict] = []
    batch_size = 500
    last_flush = time.monotonic()
    flush_interval = 5.0  # seconds

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message — flush if batch has data and interval elapsed
                if batch and (time.monotonic() - last_flush) >= flush_interval:
                    _flush_batch(conn, consumer, batch)
                    batch = []
                    last_flush = time.monotonic()
                continue

            err = msg.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(err)

            raw = msg.value()
            if raw is None:
                continue
            record = parse_message(raw, deserializer, kafka_cfg.topic_raw_ohlcv)
            if record is None:
                send_to_dlq(dlq_producer, kafka_cfg.topic_dlq, raw, "parse_error")
                continue

            batch.append(record)

            if len(batch) >= batch_size:
                _flush_batch(conn, consumer, batch)
                batch = []
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        pass
    finally:
        # Final flush
        if batch:
            _flush_batch(conn, consumer, batch)

        consumer.close()
        dlq_producer.flush(timeout=10)
        conn.close()
        logger.info("consumer_stopped")


def _flush_batch(
    conn: psycopg2.extensions.connection,
    consumer: Consumer,
    batch: list[dict],
) -> None:
    """Insert batch and commit Kafka offsets."""
    try:
        count = insert_batch(conn, batch)
        consumer.commit(asynchronous=False)
        logger.info("batch_flushed", count=count)
    except Exception:
        logger.exception("batch_flush_error")
        conn.rollback()


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    run()
