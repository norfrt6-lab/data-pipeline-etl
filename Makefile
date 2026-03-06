.PHONY: help up down logs test lint format producer consumer spark-agg spark-indicators dbt-run

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Docker ──────────────────────────────────────────────────
up: ## Start all services
	docker compose up -d

down: ## Stop all services
	docker compose down

logs: ## Tail service logs
	docker compose logs -f

# ─── Pipeline Commands ───────────────────────────────────────
producer: ## Run Kafka producer (crypto data fetcher)
	python -m src.ingestion.producer

consumer: ## Run Kafka consumer (staging loader)
	python -m src.ingestion.consumer

spark-agg: ## Run Spark OHLCV aggregation job
	spark-submit --master local[*] src/transformation/ohlcv_aggregator.py

spark-indicators: ## Run Spark technical indicators job
	spark-submit --master local[*] src/transformation/indicator_calculator.py

dbt-run: ## Run dbt models
	cd dbt_project && dbt run

dbt-test: ## Run dbt tests
	cd dbt_project && dbt test

# ─── Development ─────────────────────────────────────────────
test: ## Run tests
	pytest --cov=src --cov-report=xml -v

lint: ## Run linter
	ruff check src/ tests/

format: ## Format code
	ruff format src/ tests/

typecheck: ## Run type checker
	mypy src/

migrate: ## Run database migrations
	psql "$$DATABASE_URL" -f migrations/001_create_tables.sql
