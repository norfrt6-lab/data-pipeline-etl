"""Tests for src.monitoring.health."""

from __future__ import annotations

from unittest.mock import patch, MagicMock

from src.monitoring.health import check_health


class TestCheckHealth:
    @patch("src.monitoring.health.get_connection")
    @patch("src.monitoring.health.get_table_count")
    def test_healthy_when_postgres_up(self, mock_count, mock_conn):
        mock_count.return_value = 100
        mock_conn.return_value.__enter__ = MagicMock()
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)

        status = check_health()
        assert status["healthy"] is True
        assert "postgres" in status["components"]
        assert status["components"]["postgres"]["status"] == "up"

    @patch("src.monitoring.health.get_connection")
    def test_unhealthy_when_postgres_down(self, mock_conn):
        mock_conn.side_effect = Exception("Connection refused")

        status = check_health()
        assert status["healthy"] is False
        assert status["components"]["postgres"]["status"] == "down"
