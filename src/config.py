"""Centralized configuration loaded from environment variables."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "localhost:9092"
    topic_raw_ohlcv: str = "crypto.ohlcv.raw"
    topic_dlq: str = "crypto.ohlcv.dlq"
    consumer_group: str = "etl-consumer-group"
    auto_offset_reset: str = "earliest"

    model_config = {"env_prefix": "KAFKA_"}


class PostgresSettings(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    db: str = "crypto_warehouse"
    user: str = "pipeline"
    password: str = "pipeline_secret"

    model_config = {"env_prefix": "POSTGRES_"}

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"


class DataSourceSettings(BaseSettings):
    symbol: str = "BTC/USDT"
    timeframe: str = "1m"
    exchange: str = "binance"
    fetch_interval_seconds: int = 60

    model_config = {"env_prefix": "DATA_"}


class SparkSettings(BaseSettings):
    master: str = "local[*]"
    app_name: str = "crypto-etl"

    model_config = {"env_prefix": "SPARK_"}


class SchemaRegistrySettings(BaseSettings):
    url: str = "http://localhost:8082"

    model_config = {"env_prefix": "SCHEMA_REGISTRY_"}


class Settings(BaseSettings):
    kafka: KafkaSettings = KafkaSettings()
    postgres: PostgresSettings = PostgresSettings()
    data_source: DataSourceSettings = DataSourceSettings()
    spark: SparkSettings = SparkSettings()
    schema_registry: SchemaRegistrySettings = SchemaRegistrySettings()


def get_settings() -> Settings:
    return Settings()
