-- Raw staging table: Kafka consumer writes here
CREATE TABLE IF NOT EXISTS raw_ohlcv (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20)      NOT NULL,
    exchange        VARCHAR(30)      NOT NULL,
    timestamp_ms    BIGINT           NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    ingested_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, exchange, timestamp_ms)
);

CREATE INDEX IF NOT EXISTS idx_raw_ohlcv_symbol_ts
    ON raw_ohlcv (symbol, timestamp_ms DESC);

-- Hourly aggregated OHLCV: Spark writes here
CREATE TABLE IF NOT EXISTS ohlcv_hourly (
    id              BIGSERIAL PRIMARY KEY,
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
    UNIQUE (symbol, exchange, hour_start)
);

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
