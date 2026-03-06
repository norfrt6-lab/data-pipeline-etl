"""Tests for src.ingestion.consumer."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.ingestion.consumer import (
    _flush_batch,
    insert_batch,
    parse_message,
    send_to_dlq,
)


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

    def test_with_deserializer(self, sample_raw_value):
        mock_deser = MagicMock()
        mock_deser.deserialize_value.return_value = {
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "timestamp_ms": 1704067200000,
            "open": 42000.0,
            "high": 42100.0,
            "low": 41900.0,
            "close": 42050.0,
            "volume": 123.45,
        }
        result = parse_message(sample_raw_value, deserializer=mock_deser, topic="test")
        assert result is not None
        assert result["symbol"] == "BTC/USDT"
        mock_deser.deserialize_value.assert_called_once_with(sample_raw_value, "test")

    def test_with_deserializer_returning_none(self, sample_raw_value):
        mock_deser = MagicMock()
        mock_deser.deserialize_value.return_value = None
        result = parse_message(sample_raw_value, deserializer=mock_deser, topic="test")
        assert result is None

    def test_with_deserializer_raising_exception(self, sample_raw_value):
        mock_deser = MagicMock()
        mock_deser.deserialize_value.side_effect = Exception("decode error")
        result = parse_message(sample_raw_value, deserializer=mock_deser, topic="test")
        assert result is None


class TestInsertBatch:
    def test_empty_batch_returns_zero(self):
        result = insert_batch(MagicMock(), [])
        assert result == 0

    @patch("src.ingestion.consumer.psycopg2.extras.execute_batch")
    def test_inserts_records(self, mock_exec_batch):
        mock_conn = MagicMock()
        records = [
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
        ]
        result = insert_batch(mock_conn, records)
        assert result == 1
        mock_conn.commit.assert_called_once()
        mock_exec_batch.assert_called_once()

    @patch("src.ingestion.consumer.psycopg2.extras.execute_batch")
    def test_inserts_multiple_records(self, mock_exec_batch):
        mock_conn = MagicMock()
        records = [
            {
                "symbol": f"COIN{i}/USDT",
                "exchange": "binance",
                "timestamp_ms": 1704067200000 + i * 60000,
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 105.0,
                "volume": 50.0,
            }
            for i in range(3)
        ]
        result = insert_batch(mock_conn, records)
        assert result == 3


class TestSendToDlq:
    def test_produces_to_dlq_topic(self):
        mock_producer = MagicMock()
        send_to_dlq(mock_producer, "dlq-topic", b"bad message", "parse_error")
        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs[0][0] == "dlq-topic"
        assert call_kwargs[1]["value"] == b"bad message"
        mock_producer.poll.assert_called_once_with(0)

    def test_includes_error_reason_header(self):
        mock_producer = MagicMock()
        send_to_dlq(mock_producer, "dlq-topic", b"bad", "invalid_json")
        call_kwargs = mock_producer.produce.call_args
        headers = call_kwargs[1]["headers"]
        assert len(headers) == 1
        assert headers[0][0] == "error_reason"
        assert headers[0][1] == b"invalid_json"


class TestFlushBatch:
    def test_successful_flush(self):
        mock_conn = MagicMock()
        mock_consumer = MagicMock()
        batch = [{"symbol": "BTC/USDT"}]

        with patch("src.ingestion.consumer.insert_batch", return_value=1) as mock_insert:
            _flush_batch(mock_conn, mock_consumer, batch)
            mock_insert.assert_called_once_with(mock_conn, batch)
            mock_consumer.commit.assert_called_once_with(asynchronous=False)

    def test_flush_rollback_on_error(self):
        mock_conn = MagicMock()
        mock_consumer = MagicMock()
        batch = [{"symbol": "BTC/USDT"}]

        with patch("src.ingestion.consumer.insert_batch", side_effect=Exception("db error")):
            _flush_batch(mock_conn, mock_consumer, batch)
            mock_conn.rollback.assert_called_once()
            mock_consumer.commit.assert_not_called()


class TestConnectPostgres:
    @patch("src.ingestion.consumer.psycopg2")
    def test_successful_connection(self, mock_psycopg2):
        from src.config import PostgresSettings
        from src.ingestion.consumer import connect_postgres

        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        cfg = PostgresSettings(host="localhost", port=5432, db="test", user="u", password="p")
        conn = connect_postgres(cfg)

        assert conn is mock_conn
        assert conn.autocommit is False

    @patch("src.ingestion.consumer.psycopg2")
    @patch("src.ingestion.consumer.time")
    def test_retries_on_failure(self, mock_time, mock_psycopg2):
        from src.config import PostgresSettings
        from src.ingestion.consumer import connect_postgres

        mock_psycopg2.OperationalError = Exception
        mock_conn = MagicMock()
        mock_psycopg2.connect.side_effect = [Exception("fail"), Exception("fail"), mock_conn]

        cfg = PostgresSettings(host="localhost", port=5432, db="test", user="u", password="p")
        conn = connect_postgres(cfg)

        assert conn is mock_conn
        assert mock_psycopg2.connect.call_count == 3
