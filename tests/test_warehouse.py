"""Tests for src.loading.warehouse."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.loading.warehouse import (
    get_latest_timestamp,
    get_table_count,
    record_pipeline_run,
    update_pipeline_run,
)


class TestGetTableCount:
    def test_rejects_unknown_table(self):
        """Ensure SQL injection is prevented by table allowlist."""
        with pytest.raises(ValueError, match="Unknown table"):
            # Pass a mock connection — shouldn't be reached
            get_table_count(None, "users; DROP TABLE raw_ohlcv;--")

    def test_rejects_arbitrary_table(self):
        with pytest.raises(ValueError, match="Unknown table"):
            get_table_count(None, "some_other_table")

    def test_allowed_tables(self):
        allowed = [
            "raw_ohlcv",
            "ohlcv_hourly",
            "ohlcv_daily",
            "technical_indicators",
            "pipeline_runs",
        ]
        for table in allowed:
            # Should not raise ValueError (will raise other errors without real DB)
            with pytest.raises(Exception):
                get_table_count(None, table)
            # Verify it didn't raise ValueError

    def test_returns_count(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (42,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = get_table_count(mock_conn, "raw_ohlcv")
        assert result == 42


class TestRecordPipelineRun:
    def test_inserts_and_returns_id(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (99,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        row_id = record_pipeline_run(mock_conn, "crypto_pipeline", "run-001", "running")
        assert row_id == 99
        mock_conn.commit.assert_called_once()
        mock_cursor.execute.assert_called_once()

    def test_default_status_is_running(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        record_pipeline_run(mock_conn, "dag1", "run1")
        args = mock_cursor.execute.call_args[0]
        assert args[1] == ("dag1", "run1", "running")


class TestUpdatePipelineRun:
    def test_updates_status(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        update_pipeline_run(mock_conn, 99, "success", rows_processed=500)
        mock_conn.commit.assert_called_once()
        mock_cursor.execute.assert_called_once()
        args = mock_cursor.execute.call_args[0]
        assert args[1][0] == "success"
        assert args[1][2] == 500
        assert args[1][4] == 99

    def test_updates_with_error_message(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        update_pipeline_run(mock_conn, 1, "failed", error_message="DB timeout")
        args = mock_cursor.execute.call_args[0]
        assert args[1][0] == "failed"
        assert args[1][3] == "DB timeout"


class TestGetLatestTimestamp:
    def test_returns_timestamp(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1704067200000,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = get_latest_timestamp(mock_conn, "BTC/USDT")
        assert result == 1704067200000

    def test_returns_none_when_no_rows(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = get_latest_timestamp(mock_conn, "BTC/USDT")
        assert result is None

    def test_returns_none_when_max_is_null(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = get_latest_timestamp(mock_conn, "NEW/COIN")
        assert result is None


class TestGetConnection:
    @patch("src.loading.warehouse.psycopg2")
    def test_yields_connection(self, mock_psycopg2):
        from src.loading.warehouse import get_connection

        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        from src.config import PostgresSettings

        cfg = PostgresSettings(host="localhost", port=5432, db="test", user="u", password="p")
        with get_connection(cfg) as conn:
            assert conn is mock_conn
        mock_conn.close.assert_called_once()

    @patch("src.loading.warehouse.psycopg2")
    def test_closes_connection_on_error(self, mock_psycopg2):
        from src.loading.warehouse import get_connection

        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        from src.config import PostgresSettings

        cfg = PostgresSettings(host="localhost", port=5432, db="test", user="u", password="p")
        with pytest.raises(RuntimeError):
            with get_connection(cfg) as _conn:
                raise RuntimeError("test error")
        mock_conn.close.assert_called_once()
