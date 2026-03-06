-- Staging model: clean and type-cast raw OHLCV candles from Kafka ingestion

with source as (
    select * from {{ source('raw', 'raw_ohlcv') }}
),

cleaned as (
    select
        id,
        symbol,
        exchange,
        timestamp_ms,
        to_timestamp(timestamp_ms / 1000) at time zone 'UTC' as candle_time,
        date_trunc('hour', to_timestamp(timestamp_ms / 1000) at time zone 'UTC') as hour_bucket,
        (to_timestamp(timestamp_ms / 1000) at time zone 'UTC')::date as date_bucket,
        open,
        high,
        low,
        close,
        volume,
        ingested_at,
        -- Data quality flags
        case when high < low then true else false end as is_invalid_hl,
        case when volume < 0 then true else false end as is_negative_volume,
        case when open <= 0 or close <= 0 then true else false end as is_zero_price
    from source
)

select * from cleaned
where not is_invalid_hl
  and not is_negative_volume
  and not is_zero_price
