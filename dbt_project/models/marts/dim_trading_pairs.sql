-- Dimension: trading pairs with summary statistics

with daily as (
    select * from {{ ref('int_ohlcv_daily') }}
),

pair_stats as (
    select
        symbol,
        exchange,
        min(date) as first_date,
        max(date) as last_date,
        count(*) as total_days,
        avg(volume) as avg_daily_volume,
        avg(daily_return) as avg_daily_return,
        stddev(daily_return) as volatility,
        min(low) as all_time_low,
        max(high) as all_time_high
    from daily
    group by symbol, exchange
)

select
    *,
    -- Annualized volatility (approx 365 trading days for crypto)
    volatility * sqrt(365) as annualized_volatility,
    -- Sharpe ratio approximation (assuming 0% risk-free rate)
    case
        when volatility > 0
        then (avg_daily_return * 365) / (volatility * sqrt(365))
        else null
    end as sharpe_ratio
from pair_stats
