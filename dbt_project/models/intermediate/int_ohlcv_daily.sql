-- Intermediate model: daily OHLCV aggregation from cleaned staging data.
-- Uses first/last value window functions for proper open/close.

with staged as (
    select * from {{ ref('stg_raw_ohlcv') }}
),

daily_agg as (
    select
        symbol,
        exchange,
        date_bucket as date,
        -- Open = first candle's open price of the day
        (array_agg(open order by timestamp_ms asc))[1] as open,
        max(high) as high,
        min(low) as low,
        -- Close = last candle's close price of the day
        (array_agg(close order by timestamp_ms desc))[1] as close,
        sum(volume) as volume,
        count(*) as candle_count,
        -- VWAP
        case
            when sum(volume) > 0
            then sum(close * volume) / sum(volume)
            else null
        end as vwap,
        -- Daily return
        case
            when (array_agg(open order by timestamp_ms asc))[1] > 0
            then (
                (array_agg(close order by timestamp_ms desc))[1]
                / (array_agg(open order by timestamp_ms asc))[1]
            ) - 1
            else null
        end as daily_return
    from staged
    group by symbol, exchange, date_bucket
)

select * from daily_agg
