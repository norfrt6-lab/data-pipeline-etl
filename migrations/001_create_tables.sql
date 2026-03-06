-- Raw staging table: Kafka consumer writes here
-- Partitioned by ingested_at (monthly) for efficient time-range queries and retention
CREATE TABLE IF NOT EXISTS raw_ohlcv (
    id              BIGSERIAL       NOT NULL,
    symbol          VARCHAR(20)      NOT NULL,
    exchange        VARCHAR(30)      NOT NULL,
    timestamp_ms    BIGINT           NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    ingested_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, ingested_at),
    UNIQUE (symbol, exchange, timestamp_ms, ingested_at)
) PARTITION BY RANGE (ingested_at);

-- Create initial partitions (auto-create more via pg_partman or cron)
CREATE TABLE IF NOT EXISTS raw_ohlcv_2024_q1 PARTITION OF raw_ohlcv
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS raw_ohlcv_2024_q2 PARTITION OF raw_ohlcv
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
CREATE TABLE IF NOT EXISTS raw_ohlcv_2024_q3 PARTITION OF raw_ohlcv
    FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
CREATE TABLE IF NOT EXISTS raw_ohlcv_2024_q4 PARTITION OF raw_ohlcv
    FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS raw_ohlcv_2025_q1 PARTITION OF raw_ohlcv
    FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS raw_ohlcv_2025_q2 PARTITION OF raw_ohlcv
    FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS raw_ohlcv_2025_q3 PARTITION OF raw_ohlcv
    FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS raw_ohlcv_2025_q4 PARTITION OF raw_ohlcv
    FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS raw_ohlcv_2026_q1 PARTITION OF raw_ohlcv
    FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS raw_ohlcv_2026_q2 PARTITION OF raw_ohlcv
    FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS raw_ohlcv_default PARTITION OF raw_ohlcv DEFAULT;

CREATE INDEX IF NOT EXISTS idx_raw_ohlcv_symbol_ts
    ON raw_ohlcv (symbol, timestamp_ms DESC);

-- Hourly aggregated OHLCV: Spark writes here
-- Partitioned by hour_start (monthly) for efficient aggregation queries
CREATE TABLE IF NOT EXISTS ohlcv_hourly (
    id              BIGSERIAL       NOT NULL,
    symbol          VARCHAR(20)      NOT NULL,
    exchange        VARCHAR(30)      NOT NULL,
    hour_start      TIMESTAMPTZ      NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    trade_count     INTEGER          NOT NULL DEFAULT 0,
    vwap            DOUBLE PRECISION,
    created_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, hour_start),
    UNIQUE (symbol, exchange, hour_start)
) PARTITION BY RANGE (hour_start);

CREATE TABLE IF NOT EXISTS ohlcv_hourly_2024_h1 PARTITION OF ohlcv_hourly
    FOR VALUES FROM ('2024-01-01') TO ('2024-07-01');
CREATE TABLE IF NOT EXISTS ohlcv_hourly_2024_h2 PARTITION OF ohlcv_hourly
    FOR VALUES FROM ('2024-07-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS ohlcv_hourly_2025_h1 PARTITION OF ohlcv_hourly
    FOR VALUES FROM ('2025-01-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS ohlcv_hourly_2025_h2 PARTITION OF ohlcv_hourly
    FOR VALUES FROM ('2025-07-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS ohlcv_hourly_2026_h1 PARTITION OF ohlcv_hourly
    FOR VALUES FROM ('2026-01-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS ohlcv_hourly_default PARTITION OF ohlcv_hourly DEFAULT;

-- Daily aggregated OHLCV: Spark writes here
CREATE TABLE IF NOT EXISTS ohlcv_daily (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20)      NOT NULL,
    exchange        VARCHAR(30)      NOT NULL,
    date            DATE             NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    trade_count     INTEGER          NOT NULL DEFAULT 0,
    vwap            DOUBLE PRECISION,
    created_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, exchange, date)
);

-- Technical indicators: Spark writes here
CREATE TABLE IF NOT EXISTS technical_indicators (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20)      NOT NULL,
    date            DATE             NOT NULL,
    sma_7           DOUBLE PRECISION,
    sma_25          DOUBLE PRECISION,
    sma_99          DOUBLE PRECISION,
    ema_12          DOUBLE PRECISION,
    ema_26          DOUBLE PRECISION,
    rsi_14          DOUBLE PRECISION,
    macd            DOUBLE PRECISION,
    macd_signal     DOUBLE PRECISION,
    macd_histogram  DOUBLE PRECISION,
    bb_upper        DOUBLE PRECISION,
    bb_middle       DOUBLE PRECISION,
    bb_lower        DOUBLE PRECISION,
    atr_14          DOUBLE PRECISION,
    obv             DOUBLE PRECISION,
    created_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_indicators_symbol_date
    ON technical_indicators (symbol, date DESC);

-- Pipeline metadata
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    dag_id          VARCHAR(100)     NOT NULL,
    run_id          VARCHAR(200)     NOT NULL,
    status          VARCHAR(20)      NOT NULL DEFAULT 'running',
    started_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    rows_processed  INTEGER          DEFAULT 0,
    error_message   TEXT
);
