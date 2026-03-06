"""Airflow DAG: end-to-end crypto data pipeline.

Schedule: runs daily at 01:00 UTC (after markets have a full day of data).
Pipeline: ingest → aggregate (Spark) → indicators (Spark) → dbt run → health check.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def _run_kafka_backfill(**context: dict) -> int:
    """Fetch historical candles and publish to Kafka for backfill."""
    from src.ingestion.producer import fetch_ohlcv, publish_candles, create_producer
    from src.config import get_settings

    settings = get_settings()
    producer = create_producer(settings.kafka)

    # Fetch last 24h of candles
    candles = fetch_ohlcv(
        exchange_id=settings.data_source.exchange,
        symbol=settings.data_source.symbol,
        timeframe=settings.data_source.timeframe,
        limit=1440,  # 24h * 60 = 1440 one-minute candles
    )

    count = publish_candles(producer, settings.kafka.topic_raw_ohlcv, candles)
    producer.flush(timeout=30)
    return count


def _run_kafka_consume(**context: dict) -> int:
    """Consume all pending messages from Kafka into PostgreSQL staging."""
    import json
    from src.ingestion.consumer import (
        create_consumer,
        connect_postgres,
        insert_batch,
        parse_message,
    )
    from src.config import get_settings
    from confluent_kafka import KafkaError

    settings = get_settings()
    consumer = create_consumer(settings.kafka)
    conn = connect_postgres(settings.postgres)

    consumer.subscribe([settings.kafka.topic_raw_ohlcv])

    batch: list[dict] = []
    total = 0
    empty_polls = 0

    while empty_polls < 10:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
                continue
            raise Exception(f"Kafka error: {msg.error()}")

        empty_polls = 0
        record = parse_message(msg.value())
        if record:
            batch.append(record)

        if len(batch) >= 500:
            insert_batch(conn, batch)
            consumer.commit(asynchronous=False)
            total += len(batch)
            batch = []

    if batch:
        insert_batch(conn, batch)
        consumer.commit(asynchronous=False)
        total += len(batch)

    consumer.close()
    conn.close()
    return total


def _run_health_check(**context: dict) -> dict:
    """Run pipeline health check after all steps complete."""
    from src.monitoring.health import check_health

    status = check_health()
    if not status["healthy"]:
        raise Exception(f"Health check failed: {status}")
    return status


def _record_run_start(**context: dict) -> int:
    """Record pipeline run start in metadata table."""
    from src.loading.warehouse import get_connection, record_pipeline_run

    dag_run = context["dag_run"]
    with get_connection() as conn:
        return record_pipeline_run(conn, dag_run.dag_id, dag_run.run_id)


def _record_run_end(**context: dict) -> None:
    """Record pipeline run completion."""
    from src.loading.warehouse import get_connection, update_pipeline_run

    ti = context["ti"]
    run_db_id = ti.xcom_pull(task_ids="record_start")

    with get_connection() as conn:
        update_pipeline_run(conn, run_db_id, status="success")


with DAG(
    dag_id="crypto_etl_pipeline",
    default_args=default_args,
    description="End-to-end crypto data pipeline: ingest, transform, model, validate",
    schedule="0 1 * * *",  # Daily at 01:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "etl", "production"],
    max_active_runs=1,
) as dag:

    record_start = PythonOperator(
        task_id="record_start",
        python_callable=_record_run_start,
    )

    # ─── Ingestion ───────────────────────────────────────
    with TaskGroup("ingestion") as ingestion_group:
        backfill = PythonOperator(
            task_id="kafka_backfill",
            python_callable=_run_kafka_backfill,
        )

        consume = PythonOperator(
            task_id="kafka_consume",
            python_callable=_run_kafka_consume,
        )

        backfill >> consume

    # ─── Spark Transformations ───────────────────────────
    with TaskGroup("transformation") as transform_group:
        aggregate = BashOperator(
            task_id="spark_aggregate",
            bash_command="spark-submit --master local[*] /opt/airflow/src/transformation/ohlcv_aggregator.py",
        )

        indicators = BashOperator(
            task_id="spark_indicators",
            bash_command="spark-submit --master local[*] /opt/airflow/src/transformation/indicator_calculator.py",
        )

        aggregate >> indicators

    # ─── dbt Models ──────────────────────────────────────
    with TaskGroup("dbt_models") as dbt_group:
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir .",
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir .",
        )

        dbt_run >> dbt_test

    # ─── Validation ──────────────────────────────────────
    health_check = PythonOperator(
        task_id="health_check",
        python_callable=_run_health_check,
    )

    record_end = PythonOperator(
        task_id="record_end",
        python_callable=_record_run_end,
        trigger_rule="all_success",
    )

    # DAG dependencies
    record_start >> ingestion_group >> transform_group >> dbt_group >> health_check >> record_end
