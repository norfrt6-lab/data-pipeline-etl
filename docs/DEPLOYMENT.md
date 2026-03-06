# Deployment Guide

## Prerequisites

- Docker and Docker Compose v2
- Python 3.10+ (for local development)
- 8 GB RAM minimum (Spark + Kafka + Airflow)

## Local Development

```bash
# Copy environment template
cp .env.example .env

# Start all services
docker compose up -d

# Wait for Kafka topics to be created
docker compose logs -f kafka-init

# Run database migrations
docker compose exec postgres psql -U postgres -d crypto_warehouse -f /docker-entrypoint-initdb.d/001_create_tables.sql

# Seed historical data
pip install -r requirements.txt
python scripts/seed_historical.py

# Verify pipeline
make spark-agg
make spark-indicators
make dbt-run
```

## Production Deployment

### Infrastructure Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| Kafka | 3 brokers, 3 partitions | 5 brokers, 6 partitions |
| PostgreSQL | 2 vCPU, 8 GB RAM, 100 GB SSD | 4 vCPU, 16 GB, 500 GB SSD |
| Spark | 1 master + 2 workers (4 cores, 8 GB each) | 1 master + 4 workers |
| Airflow | 1 webserver + 1 scheduler + 2 workers | Auto-scaling workers |

### Environment Variables

Set these in your deployment platform (ECS, Kubernetes, etc.):

```bash
# Required
KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
POSTGRES_HOST=your-rds-endpoint.amazonaws.com
POSTGRES_DB=crypto_warehouse
POSTGRES_USER=pipeline_user
POSTGRES_PASSWORD=<secret>
SCHEMA_REGISTRY_URL=http://schema-registry:8082
SPARK_MASTER=spark://spark-master:7077

# Optional
DATA_SYMBOL=BTC/USDT
DATA_EXCHANGE=binance
DATA_TIMEFRAME=1m
DATA_FETCH_INTERVAL=60
```

### Database Setup

Run migrations before first deployment:

```bash
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f migrations/001_create_tables.sql
```

Create partitions for future quarters as needed. The default partition catches any data outside predefined ranges.

### Monitoring Setup

1. Point Prometheus at your Kafka and PostgreSQL exporters.
2. Import `docker/grafana/dashboards/pipeline-overview.json` into Grafana.
3. Set up alerts for:
   - `pipeline_errors_total` rate > 0 for 5 minutes
   - `pipeline_table_rows{table="raw_ohlcv"}` not increasing for 1 hour
   - Consumer lag > 10,000 messages

### Health Checks

The pipeline exposes a health check via `src/monitoring/health.py`:

```python
from src.monitoring.health import check_health
status = check_health()
# {"healthy": true, "components": {"postgres": {"status": "up", ...}}}
```

Airflow runs this after every DAG execution.

## Scaling

### Kafka

- Add partitions to `crypto.ohlcv.raw` for higher throughput.
- Increase `batch.size` and `linger.ms` on the producer for better batching.
- Add consumer instances (same `group.id`) for parallel consumption.

### Spark

- Add worker nodes: `docker compose up --scale spark-worker=4`.
- Increase `driver_memory` and `executor_memory` in the Airflow DAG.
- Tune `spark.sql.shuffle.partitions` based on data volume.

### PostgreSQL

- Add read replicas for analytics queries.
- Increase `shared_buffers` and `work_mem` for large aggregations.
- Schedule `VACUUM ANALYZE` on partitioned tables.

## Rollback

1. Revert to the previous Docker image tag.
2. Database migrations are additive — no destructive changes.
3. Kafka topics retain messages for the configured retention period (default 7 days).
4. dbt models can be re-run at any time (`make dbt-run`).
