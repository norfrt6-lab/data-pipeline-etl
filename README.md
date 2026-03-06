# Data Pipeline & ETL

Real-time and batch data pipeline for crypto market data. Ingests BTC/USDT OHLCV candles via Kafka, transforms with PySpark, models with dbt, and orchestrates with Apache Airflow.

## Quick Start

```bash
# Start all services
docker compose up -d

# Wait for initialization (~30s)
docker compose logs -f kafka-init

# Seed historical data
python scripts/seed_historical.py

# Run the pipeline
make spark-agg
make spark-indicators
make dbt-run
```

## Architecture

```
Binance API → Kafka Producer → Kafka → Consumer → PostgreSQL (staging)
                                                        ↓
                                                   PySpark Jobs
                                                   (aggregate + indicators)
                                                        ↓
                                                   PostgreSQL (warehouse)
                                                        ↓
                                                   dbt Models
                                                   (staging → intermediate → marts)
                                                        ↓
                                                   Airflow DAG
                                                   (orchestration + monitoring)
```

### Pipeline Stages

| Stage | Tool | Input | Output |
|---|---|---|---|
| 1. Ingest | Kafka Producer + CCXT | Binance public API | `crypto.ohlcv.raw` topic |
| 2. Stage | Kafka Consumer | Kafka topic | `raw_ohlcv` table |
| 3. Aggregate | PySpark | `raw_ohlcv` | `ohlcv_hourly`, `ohlcv_daily` |
| 4. Indicators | PySpark | `ohlcv_daily` | `technical_indicators` |
| 5. Model | dbt | Warehouse tables | `fct_market_daily`, `dim_trading_pairs` |
| 6. Orchestrate | Airflow | All stages | Pipeline metadata + health checks |

## Project Structure

```
data-pipeline-etl/
├── src/
│   ├── config.py                  # Pydantic settings from env vars
│   ├── ingestion/
│   │   ├── producer.py            # Kafka producer (CCXT → Kafka)
│   │   └── consumer.py            # Kafka consumer (Kafka → PostgreSQL)
│   ├── transformation/
│   │   ├── ohlcv_aggregator.py    # PySpark: minute → hourly/daily
│   │   └── indicator_calculator.py # PySpark: SMA, RSI, MACD, BB, ATR, OBV
│   ├── loading/
│   │   └── warehouse.py           # PostgreSQL utilities, metadata tracking
│   └── monitoring/
│       └── health.py              # Health checks, Prometheus metrics
├── dags/
│   └── crypto_pipeline.py         # Airflow DAG (daily schedule)
├── dbt_project/
│   ├── models/
│   │   ├── staging/               # stg_raw_ohlcv (validation, quality flags)
│   │   ├── intermediate/          # int_ohlcv_daily (aggregation, VWAP, returns)
│   │   └── marts/                 # fct_market_daily, dim_trading_pairs
│   ├── macros/                    # SQL helpers (SMA, pct_change, rolling_std)
│   └── tests/                     # Data quality assertions
├── migrations/
│   └── 001_create_tables.sql      # DDL for all warehouse tables
├── tests/                         # pytest suite (35+ tests)
├── docker/
│   ├── airflow/Dockerfile
│   └── spark/Dockerfile
├── docker-compose.yml             # 10-service stack
├── Makefile                       # Development shortcuts
└── .github/workflows/ci.yml       # Lint + test CI pipeline
```

## Data Model

### Staging Layer
- **raw_ohlcv** — Raw minute-level candles from Kafka consumer. Deduplication via `UNIQUE(symbol, exchange, timestamp_ms)`.
- **stg_raw_ohlcv** (dbt view) — Cleaned data with quality flags (invalid high/low, negative volume, zero price).

### Warehouse Layer
- **ohlcv_hourly** — Hourly candles with VWAP. Uses window functions for proper first/last open/close.
- **ohlcv_daily** — Daily candles with VWAP and trade count.
- **technical_indicators** — 15+ indicators per symbol per day: SMA(7/25/99), EMA(12/26), RSI(14), MACD, Bollinger Bands, ATR(14), OBV.

### Analytics Layer (dbt)
- **int_ohlcv_daily** — Daily OHLCV with VWAP and daily returns.
- **fct_market_daily** — Daily facts joined with indicators plus derived signals (RSI overbought/oversold, MACD direction, Bollinger Band position).
- **dim_trading_pairs** — Pair-level statistics: volatility, annualized volatility, Sharpe ratio.

### Metadata
- **pipeline_runs** — DAG run tracking with status, row counts, and error messages.

## Docker Compose Services

| Service | Image | Port | Purpose |
|---|---|---|---|
| Zookeeper | confluentinc/cp-zookeeper:7.7.1 | 2181 | Kafka coordination |
| Kafka | confluentinc/cp-kafka:7.7.1 | 9092 | Message broker |
| PostgreSQL | postgres:16-alpine | 5432 | Data warehouse |
| Redis | redis:7-alpine | 6379 | Airflow Celery backend |
| Spark Master | bitnami/spark:3.5 | 8080, 7077 | Spark cluster manager |
| Spark Worker | bitnami/spark:3.5 | — | Spark executor |
| Airflow Webserver | Custom | 8081 | Airflow UI |
| Airflow Scheduler | Custom | — | DAG scheduling |
| Airflow Worker | Custom | — | Task execution |

## Kafka Topics

| Topic | Partitions | Purpose |
|---|---|---|
| `crypto.ohlcv.raw` | 3 | Raw OHLCV candles from producer |
| `crypto.ohlcv.dlq` | 1 | Dead letter queue for parse failures |

## Configuration

All settings via environment variables (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_DB` | `crypto_warehouse` | Database name |
| `DATA_SYMBOL` | `BTC/USDT` | Trading pair |
| `DATA_EXCHANGE` | `binance` | Exchange (CCXT format) |
| `SPARK_MASTER` | `local[*]` | Spark master URL |

## Technical Indicators

Computed by PySpark on daily OHLCV data:

| Category | Indicators |
|---|---|
| Moving Averages | SMA(7), SMA(25), SMA(99), EMA(12), EMA(26) |
| Oscillators | RSI(14), MACD, MACD Signal, MACD Histogram |
| Volatility | Bollinger Bands (upper/middle/lower), ATR(14) |
| Volume | On-Balance Volume (OBV) |

## dbt Macros

Reusable SQL helpers in `dbt_project/macros/`:

- `sma(column, period)` — Simple moving average window function
- `pct_change(column, periods)` — Percentage change from N periods ago
- `rolling_std(column, period)` — Rolling standard deviation

## Airflow DAG

The `crypto_etl_pipeline` DAG runs daily at 01:00 UTC:

```
record_start → [ingestion] → [transformation] → [dbt_models] → health_check → record_end
                  ├─ kafka_backfill   ├─ spark_aggregate    ├─ dbt_run
                  └─ kafka_consume    └─ spark_indicators   └─ dbt_test
```

- **CeleryExecutor** for parallel task execution
- **Retries** (2x with 5-minute delay)
- **Execution timeout** (1 hour)
- **Metadata tracking** in `pipeline_runs` table

## Development

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Run tests
make test

# Lint
make lint

# Format
make format

# Type check
make typecheck
```

## Extending

**Add a new trading pair:** Set `DATA_SYMBOL=ETH/USDT` in `.env`. The pipeline handles multiple symbols — partition by symbol is built into all queries.

**Add a new indicator:** Add a `compute_*` function in `src/transformation/indicator_calculator.py`, call it from `compute_all_indicators()`, and add the column to the `technical_indicators` table migration.

**Add a new dbt model:** Create a `.sql` file in the appropriate layer (`staging/`, `intermediate/`, `marts/`) following existing patterns. Add schema tests in a corresponding `.yml` file.
