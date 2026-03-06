# Changelog

## [0.1.0] - 2024-01-01

### Added
- Real-time ingestion pipeline with Kafka producer and consumer
- CCXT integration for Binance OHLCV data
- Avro serialization with Schema Registry and JSON fallback
- PySpark aggregation jobs (minute → hourly/daily)
- 15+ technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands, ATR, OBV)
- dbt models: staging, intermediate, and mart layers
- Airflow DAG with task groups and metadata tracking
- PostgreSQL warehouse with range partitioning
- Dead letter queue for failed messages
- Docker Compose stack (Kafka, Spark, Airflow, PostgreSQL, Prometheus, Grafana)
- CI pipeline with ruff, mypy, pytest, and integration tests
- Prometheus metrics and Grafana dashboard
- Historical data seeding script
- 86 unit tests with 69.85% coverage
- 4 integration tests using testcontainers
