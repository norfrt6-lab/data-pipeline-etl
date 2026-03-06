-- Test: ensure no negative volumes exist in the daily mart
select date, symbol, volume
from {{ ref('fct_market_daily') }}
where volume < 0
