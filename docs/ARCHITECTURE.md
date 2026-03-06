# Architecture

## System Overview

```
┌──────────────┐     ┌──────────┐     ┌──────────────┐     ┌──────────────┐
│  Binance API │────▸│  Kafka   │────▸│  PostgreSQL  │────▸│   PySpark    │
│  (CCXT)      │     │  Broker  │     │  (staging)   │     │  (transform) │
└──────────────┘     └──────────┘     └──────────────┘     └──────────────┘
                          │                                       │
                     ┌────▾────┐                            ┌─────▾──────┐
                     │   DLQ   │                            │ PostgreSQL │
                     │  Topic  │                            │ (warehouse)│
                     └─────────┘                            └─────┬──────┘
                                                                  │
                                                            ┌─────▾──────┐
                                                            │    dbt     │
                                                            │  (models)  │
                                                            └─────┬──────┘
                                                                  │
                     ┌──────────┐     ┌──────────────┐      ┌────▾───────┐
                     │ Grafana  │◂────│  Prometheus  │      │  Airflow   │
                     │ (viz)    │     │  (metrics)   │      │  (orch)    │
                     └──────────┘     └──────────────┘      └────────────┘
```

## Design Decisions

### Why Kafka over direct API polling?

- **Decoupling**: Producer and consumer operate independently. If the database is down, messages queue in Kafka.
- **Replay**: Kafka retains messages for configurable periods, enabling reprocessing without hitting the API again.
- **Dead Letter Queue**: Parse failures route to a separate topic for later inspection rather than dropping data.

### Why Avro with Schema Registry?

- **Schema evolution**: Fields can be added/removed without breaking consumers.
- **Compact serialization**: Avro binary is smaller than JSON for high-throughput topics.
- **Fallback design**: JSON serialization kicks in automatically when the registry is unreachable, so the pipeline never hard-fails on schema infrastructure.

### Why PySpark over plain SQL?

- **Horizontal scaling**: Spark distributes computation across a cluster. Daily aggregation and 15+ indicator calculations benefit from parallelism.
- **EMA accuracy**: True exponential moving averages require sequential state. Spark's `applyInPandas` runs per-symbol Pandas UDFs while Spark handles partitioning and scheduling.
- **Testability**: Spark DataFrames are testable with local sessions — no database required.

### Why dbt on top of Spark?

Spark handles heavy computation (aggregation, indicators). dbt handles the analytics layer:
- **Staging views** add data quality flags without duplicating data.
- **Intermediate models** join and reshape for analyst consumption.
- **Mart models** derive trading signals (RSI overbought, MACD crossover) and dimension tables (pair-level statistics).

dbt's schema tests (`unique`, `not_null`, `accepted_values`) enforce data contracts that Spark jobs don't.

### Why PostgreSQL range partitioning?

- **raw_ohlcv** partitioned quarterly by `ingested_at` — enables fast pruning for recent data queries and efficient retention management (drop old partitions).
- **ohlcv_hourly** partitioned semi-annually by `hour_start` — balances partition count with query performance.
- Default partitions catch any data outside predefined ranges.

### Why CeleryExecutor for Airflow?

- SparkSubmitOperator and BashOperator tasks can run in parallel across Celery workers.
- Redis as the message broker is lightweight and doesn't require another Kafka cluster.
- Scales horizontally by adding more Airflow worker containers.

## Data Flow

### Real-time Path (Producer → Consumer)

1. **Producer** polls Binance via CCXT every 60 seconds for BTC/USDT 1-minute candles.
2. Each candle is serialized (Avro or JSON) and published to `crypto.ohlcv.raw` with key `{symbol}:{timestamp_ms}`.
3. Deduplication: only candles with `timestamp_ms > last_seen` are published.
4. **Consumer** polls the topic, batch-deserializes, validates required fields, and upserts into `raw_ohlcv` using `execute_batch` with `ON CONFLICT DO NOTHING`.
5. Failed messages are routed to `crypto.ohlcv.dlq` with an `error_reason` header.
6. Kafka offsets are committed only after a successful database commit (at-least-once semantics).

### Batch Path (Spark → dbt)

1. **OHLCV Aggregator** reads `raw_ohlcv` via JDBC, groups by symbol and time bucket, computes OHLCV + VWAP, writes to `ohlcv_hourly` and `ohlcv_daily`.
2. **Indicator Calculator** reads `ohlcv_daily`, computes SMA/EMA/RSI/MACD/BB/ATR/OBV per symbol, writes to `technical_indicators`.
3. **dbt** runs staging → intermediate → mart models, applies schema tests.

### Orchestration

Airflow DAG `crypto_etl_pipeline` runs daily at 01:00 UTC:

```
record_start → ingestion → transformation → dbt_models → health_check → record_end
```

Each run is tracked in `pipeline_runs` with status, row counts, and error messages.

## Monitoring

| Metric | Type | Source |
|--------|------|--------|
| `pipeline_messages_produced_total` | Counter | Producer |
| `pipeline_messages_consumed_total` | Counter | Consumer |
| `pipeline_batch_insert_seconds` | Histogram | Consumer |
| `pipeline_table_rows` | Gauge | Health check |
| `pipeline_errors_total` | Counter | All stages |

Prometheus scrapes Kafka Exporter (broker/topic/consumer-group metrics) and PostgreSQL Exporter (connection/query metrics). Grafana dashboards visualize throughput, latency, and table growth.

## Failure Modes

| Failure | Behavior | Recovery |
|---------|----------|----------|
| Kafka broker down | Producer/consumer retry with backoff | Automatic reconnection |
| PostgreSQL down | Consumer retries (5 attempts, exponential backoff) | Automatic reconnection |
| Schema Registry down | Serialization falls back to JSON | Transparent, no data loss |
| Spark job fails | Airflow retries (2x, 5-minute delay) | Automatic retry |
| Parse error | Message routed to DLQ topic | Manual inspection |
| Binance API rate limit | CCXT `enableRateLimit` throttles requests | Automatic |
