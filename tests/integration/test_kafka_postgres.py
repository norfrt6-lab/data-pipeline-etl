"""Integration tests using testcontainers for Kafka and PostgreSQL.

These tests spin up real containers to verify the end-to-end flow:
  1. Producer → Kafka topic
  2. Kafka topic → Consumer → PostgreSQL
  3. Data integrity checks
"""

from __future__ import annotations

import json
from urllib.parse import urlparse

import psycopg2
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer


def _pg_connect(url: str) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL from a testcontainers URL."""
    parsed = urlparse(url)
    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port,
        dbname=parsed.path.lstrip("/"),
        user=parsed.username,
        password=parsed.password,
    )


@pytest.fixture(scope="module")
def postgres_container():
    """Start a PostgreSQL container for integration tests."""
    with PostgresContainer("postgres:16-alpine") as pg:
        conn = _pg_connect(pg.get_connection_url())
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_ohlcv (
                    id              BIGSERIAL PRIMARY KEY,
                    symbol          VARCHAR(20)      NOT NULL,
                    exchange        VARCHAR(30)      NOT NULL,
                    timestamp_ms    BIGINT           NOT NULL,
                    open            DOUBLE PRECISION NOT NULL,
                    high            DOUBLE PRECISION NOT NULL,
                    low             DOUBLE PRECISION NOT NULL,
                    close           DOUBLE PRECISION NOT NULL,
                    volume          DOUBLE PRECISION NOT NULL,
                    ingested_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
                    UNIQUE (symbol, exchange, timestamp_ms)
                )
            """)
        conn.close()
        yield pg


@pytest.fixture(scope="module")
def kafka_container():
    """Start a Kafka container for integration tests."""
    with KafkaContainer("confluentinc/cp-kafka:7.7.1") as kafka:
        # Create the test topic
        admin = AdminClient({"bootstrap.servers": kafka.get_bootstrap_server()})
        topic = NewTopic("crypto.ohlcv.raw", num_partitions=1, replication_factor=1)
        futures = admin.create_topics([topic])
        for _, future in futures.items():
            future.result(timeout=10)
        yield kafka


def _make_candle(symbol: str = "BTC/USDT", ts_ms: int = 1700000000000) -> dict:
    return {
        "symbol": symbol,
        "exchange": "binance",
        "timeframe": "1m",
        "timestamp_ms": ts_ms,
        "open": 42000.0,
        "high": 42100.0,
        "low": 41900.0,
        "close": 42050.0,
        "volume": 123.45,
        "fetched_at": "2024-01-01T00:00:00Z",
    }


@pytest.mark.integration
class TestKafkaProduceConsume:
    """Test producing to Kafka and consuming back."""

    def test_produce_and_consume(self, kafka_container):
        bootstrap = kafka_container.get_bootstrap_server()
        topic = "crypto.ohlcv.raw"

        # Produce
        producer = Producer({"bootstrap.servers": bootstrap})
        candle = _make_candle()
        producer.produce(topic, key=b"BTC/USDT:1700000000000", value=json.dumps(candle).encode())
        producer.flush(timeout=10)

        # Consume
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "test-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])

        msg = None
        for _ in range(30):
            msg = consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                break

        consumer.close()

        assert msg is not None
        assert not msg.error()
        data = json.loads(msg.value().decode())
        assert data["symbol"] == "BTC/USDT"
        assert data["close"] == 42050.0


@pytest.mark.integration
class TestPostgresInsert:
    """Test inserting candle data into PostgreSQL."""

    def test_batch_insert(self, postgres_container):
        conn = _pg_connect(postgres_container.get_connection_url())
        conn.autocommit = False

        candles = [_make_candle(ts_ms=1700000000000 + i * 60000) for i in range(10)]

        sql = """
            INSERT INTO raw_ohlcv (symbol, exchange, timestamp_ms, open, high, low, close, volume)
            VALUES (%(symbol)s, %(exchange)s, %(timestamp_ms)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
            ON CONFLICT (symbol, exchange, timestamp_ms) DO NOTHING
        """

        with conn.cursor() as cur:
            for candle in candles:
                cur.execute(sql, candle)

        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_ohlcv")
            count = cur.fetchone()[0]

        assert count == 10
        conn.close()

    def test_upsert_deduplication(self, postgres_container):
        conn = _pg_connect(postgres_container.get_connection_url())
        conn.autocommit = False

        # Insert same candle twice — should not duplicate
        candle = _make_candle(ts_ms=9999999999999)

        sql = """
            INSERT INTO raw_ohlcv (symbol, exchange, timestamp_ms, open, high, low, close, volume)
            VALUES (%(symbol)s, %(exchange)s, %(timestamp_ms)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
            ON CONFLICT (symbol, exchange, timestamp_ms) DO NOTHING
        """

        with conn.cursor() as cur:
            cur.execute(sql, candle)
            conn.commit()
            cur.execute(sql, candle)
            conn.commit()
            cur.execute(
                "SELECT COUNT(*) FROM raw_ohlcv WHERE timestamp_ms = %s",
                (9999999999999,),
            )
            count = cur.fetchone()[0]

        assert count == 1
        conn.close()


@pytest.mark.integration
class TestEndToEnd:
    """Full pipeline: produce → Kafka → consume → PostgreSQL."""

    def test_produce_consume_insert(self, kafka_container, postgres_container):
        bootstrap = kafka_container.get_bootstrap_server()
        conn_url = postgres_container.get_connection_url()
        topic = "crypto.ohlcv.raw"

        # Produce 5 candles
        producer = Producer({"bootstrap.servers": bootstrap})
        candles = [_make_candle(ts_ms=1800000000000 + i * 60000) for i in range(5)]
        for candle in candles:
            key = f"{candle['symbol']}:{candle['timestamp_ms']}"
            producer.produce(topic, key=key.encode(), value=json.dumps(candle).encode())
        producer.flush(timeout=10)

        # Consume and insert
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "e2e-test-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])

        conn = _pg_connect(conn_url)
        conn.autocommit = False
        consumed = []

        for _ in range(60):
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                if len(consumed) >= 5:
                    break
                continue
            data = json.loads(msg.value().decode())
            consumed.append(data)
            if len(consumed) >= 5:
                break

        consumer.close()

        sql = """
            INSERT INTO raw_ohlcv (symbol, exchange, timestamp_ms, open, high, low, close, volume)
            VALUES (%(symbol)s, %(exchange)s, %(timestamp_ms)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
            ON CONFLICT (symbol, exchange, timestamp_ms) DO NOTHING
        """
        with conn.cursor() as cur:
            for record in consumed:
                cur.execute(sql, record)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM raw_ohlcv WHERE timestamp_ms >= %s",
                (1800000000000,),
            )
            count = cur.fetchone()[0]

        assert count == 5
        conn.close()
