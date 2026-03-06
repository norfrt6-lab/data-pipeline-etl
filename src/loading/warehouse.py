"""Warehouse loading utilities for direct PostgreSQL operations.

Used by Airflow DAGs and scripts for metadata tracking, health checks,
and lightweight SQL operations outside of Spark.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import structlog

from src.config import PostgresSettings, get_settings

logger = structlog.get_logger(__name__)


@contextmanager
def get_connection(
    pg_cfg: PostgresSettings | None = None,
) -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager for PostgreSQL connections."""
    if pg_cfg is None:
        pg_cfg = get_settings().postgres

    conn = psycopg2.connect(
        host=pg_cfg.host,
        port=pg_cfg.port,
        dbname=pg_cfg.db,
        user=pg_cfg.user,
        password=pg_cfg.password,
    )
    try:
        yield conn
    finally:
        conn.close()


def record_pipeline_run(
    conn: psycopg2.extensions.connection,
    dag_id: str,
    run_id: str,
    status: str = "running",
) -> int:
    """Insert a pipeline run record. Returns the row ID."""
    sql = """
        INSERT INTO pipeline_runs (dag_id, run_id, status)
        VALUES (%s, %s, %s)
        RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (dag_id, run_id, status))
        row = cur.fetchone()
        assert row is not None
        row_id: int = row[0]
    conn.commit()
    return row_id


def update_pipeline_run(
    conn: psycopg2.extensions.connection,
    run_db_id: int,
    status: str,
    rows_processed: int = 0,
    error_message: str | None = None,
) -> None:
    """Update a pipeline run record with final status."""
    sql = """
        UPDATE pipeline_runs
        SET status = %s,
            finished_at = %s,
            rows_processed = %s,
            error_message = %s
        WHERE id = %s
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (status, datetime.now(timezone.utc), rows_processed, error_message, run_db_id),
        )
    conn.commit()


def get_table_count(conn: psycopg2.extensions.connection, table: str) -> int:
    """Get row count for a table (for health checks)."""
    # Sanitize table name to prevent SQL injection
    allowed_tables = {
        "raw_ohlcv",
        "ohlcv_hourly",
        "ohlcv_daily",
        "technical_indicators",
        "pipeline_runs",
    }
    if table not in allowed_tables:
        raise ValueError(f"Unknown table: {table}")

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
        row = cur.fetchone()
        assert row is not None
        result: int = row[0]
        return result


def get_latest_timestamp(conn: psycopg2.extensions.connection, symbol: str) -> int | None:
    """Get the latest timestamp_ms for a symbol in raw_ohlcv."""
    sql = "SELECT MAX(timestamp_ms) FROM raw_ohlcv WHERE symbol = %s"
    with conn.cursor() as cur:
        cur.execute(sql, (symbol,))
        row = cur.fetchone()
        if row is None:
            return None
        result: int | None = row[0]
    return result
