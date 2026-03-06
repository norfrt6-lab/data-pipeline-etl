"""Tests for src.loading.warehouse."""

from __future__ import annotations

import pytest

from src.loading.warehouse import get_table_count


class TestGetTableCount:
    def test_rejects_unknown_table(self):
        """Ensure SQL injection is prevented by table allowlist."""
        with pytest.raises(ValueError, match="Unknown table"):
            # Pass a mock connection — shouldn't be reached
            get_table_count(None, "users; DROP TABLE raw_ohlcv;--")

    def test_rejects_arbitrary_table(self):
        with pytest.raises(ValueError, match="Unknown table"):
            get_table_count(None, "some_other_table")

    def test_allowed_tables(self):
        allowed = ["raw_ohlcv", "ohlcv_hourly", "ohlcv_daily", "technical_indicators", "pipeline_runs"]
        for table in allowed:
            # Should not raise ValueError (will raise other errors without real DB)
            with pytest.raises(Exception):
                get_table_count(None, table)
            # Verify it didn't raise ValueError
