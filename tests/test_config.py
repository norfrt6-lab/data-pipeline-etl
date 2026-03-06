"""Tests for src.config."""

from __future__ import annotations

from src.config import (
    DataSourceSettings,
    KafkaSettings,
    PostgresSettings,
    Settings,
    get_settings,
)


class TestKafkaSettings:
    def test_defaults(self):
        cfg = KafkaSettings()
        assert cfg.bootstrap_servers == "localhost:9092"
        assert cfg.topic_raw_ohlcv == "crypto.ohlcv.raw"
        assert cfg.consumer_group == "etl-consumer-group"

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9093")
        cfg = KafkaSettings()
        assert cfg.bootstrap_servers == "broker:9093"


class TestPostgresSettings:
    def test_url_property(self):
        cfg = PostgresSettings()
        assert "postgresql://pipeline:pipeline_secret@localhost:5432/crypto_warehouse" == cfg.url

    def test_jdbc_url_property(self):
        cfg = PostgresSettings()
        assert cfg.jdbc_url == "jdbc:postgresql://localhost:5432/crypto_warehouse"

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "db-host")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        cfg = PostgresSettings()
        assert "db-host" in cfg.url
        assert "5433" in cfg.url


class TestDataSourceSettings:
    def test_defaults(self):
        cfg = DataSourceSettings()
        assert cfg.symbol == "BTC/USDT"
        assert cfg.exchange == "binance"
        assert cfg.fetch_interval_seconds == 60


class TestSettings:
    def test_nested_settings(self):
        s = Settings()
        assert isinstance(s.kafka, KafkaSettings)
        assert isinstance(s.postgres, PostgresSettings)
        assert isinstance(s.data_source, DataSourceSettings)

    def test_get_settings(self):
        s = get_settings()
        assert isinstance(s, Settings)
