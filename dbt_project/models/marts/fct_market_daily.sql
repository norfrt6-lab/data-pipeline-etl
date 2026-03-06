-- Mart: daily market facts combining OHLCV with technical indicators.
-- This is the primary analytics table for dashboards and reporting.

with daily as (
    select * from {{ ref('int_ohlcv_daily') }}
),

indicators as (
    select * from {{ source('warehouse', 'technical_indicators') }}
),

joined as (
    select
        d.symbol,
        d.exchange,
        d.date,
        d.open,
        d.high,
        d.low,
        d.close,
        d.volume,
        d.candle_count,
        d.vwap,
        d.daily_return,

        -- Moving averages
        i.sma_7,
        i.sma_25,
        i.sma_99,
        i.ema_12,
        i.ema_26,

        -- Oscillators
        i.rsi_14,
        i.macd,
        i.macd_signal,
        i.macd_histogram,

        -- Volatility
        i.bb_upper,
        i.bb_middle,
        i.bb_lower,
        i.atr_14,

        -- Volume
        i.obv,

        -- Derived signals
        case
            when i.rsi_14 < 30 then 'oversold'
            when i.rsi_14 > 70 then 'overbought'
            else 'neutral'
        end as rsi_signal,

        case
            when i.macd > i.macd_signal then 'bullish'
            when i.macd < i.macd_signal then 'bearish'
            else 'neutral'
        end as macd_signal_direction,

        case
            when d.close > i.bb_upper then 'above_upper'
            when d.close < i.bb_lower then 'below_lower'
            else 'within_bands'
        end as bb_position

    from daily d
    left join indicators i
        on d.symbol = i.symbol
        and d.date = i.date
)

select * from joined
