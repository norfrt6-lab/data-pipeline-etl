"""Health check and monitoring utilities.

Exposes Prometheus metrics and a health check function for pipeline status.
"""

from __future__ import annotations

import structlog
from prometheus_client import Counter, Gauge, Histogram

from src.loading.warehouse import get_connection, get_table_count

logger = structlog.get_logger(__name__)

# Prometheus metrics
messages_produced = Counter(
    "pipeline_messages_produced_total",
    "Total messages published to Kafka",
    ["topic"],
)

messages_consumed = Counter(
    "pipeline_messages_consumed_total",
    "Total messages consumed from Kafka",
    ["topic"],
)

batch_insert_duration = Histogram(
    "pipeline_batch_insert_seconds",
    "Time spent inserting a batch into PostgreSQL",
    ["table"],
)

rows_in_table = Gauge(
    "pipeline_table_rows",
    "Current row count per table",
    ["table"],
)

pipeline_errors = Counter(
    "pipeline_errors_total",
    "Total pipeline errors",
    ["stage"],
)


def check_health() -> dict:
    """Run health checks against all pipeline components. Returns status dict."""
    status: dict = {"healthy": True, "components": {}}

    # PostgreSQL
    try:
        with get_connection() as conn:
            tables = ["raw_ohlcv", "ohlcv_hourly", "ohlcv_daily", "technical_indicators"]
            counts = {}
            for table in tables:
                count = get_table_count(conn, table)
                counts[table] = count
                rows_in_table.labels(table=table).set(count)

            status["components"]["postgres"] = {"status": "up", "table_counts": counts}
    except Exception as exc:
        status["healthy"] = False
        status["components"]["postgres"] = {"status": "down", "error": str(exc)}

    return status
