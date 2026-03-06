"""Seed the pipeline with historical BTC/USDT data.

Fetches daily candles from Binance via CCXT and inserts directly into
raw_ohlcv for initial pipeline testing without running Kafka.
"""

from __future__ import annotations

import sys
from datetime import datetime

import ccxt
import psycopg2
import psycopg2.extras

from src.config import get_settings


def main() -> None:
    settings = get_settings()
    pg = settings.postgres

    # Fetch historical data
    exchange = ccxt.binance({"enableRateLimit": True})
    since = exchange.parse8601("2023-01-01T00:00:00Z")

    print(f"Fetching BTC/USDT daily candles from {settings.data_source.exchange}...")
    all_candles = []
    while since < exchange.milliseconds():
        candles = exchange.fetch_ohlcv("BTC/USDT", "1d", since=since, limit=500)
        if not candles:
            break
        all_candles.extend(candles)
        since = candles[-1][0] + 86400000  # Next day
        print(f"  Fetched {len(all_candles)} candles so far...")

    print(f"Total: {len(all_candles)} candles")

    # Insert into PostgreSQL
    conn = psycopg2.connect(
        host=pg.host, port=pg.port, dbname=pg.db, user=pg.user, password=pg.password
    )

    records = [
        {
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "timestamp_ms": c[0],
            "open": c[1],
            "high": c[2],
            "low": c[3],
            "close": c[4],
            "volume": c[5],
        }
        for c in all_candles
    ]

    sql = """
        INSERT INTO raw_ohlcv (symbol, exchange, timestamp_ms, open, high, low, close, volume)
        VALUES (%(symbol)s, %(exchange)s, %(timestamp_ms)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
        ON CONFLICT (symbol, exchange, timestamp_ms) DO NOTHING
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=500)

    conn.commit()
    conn.close()
    print(f"Inserted {len(records)} records into raw_ohlcv")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    main()
