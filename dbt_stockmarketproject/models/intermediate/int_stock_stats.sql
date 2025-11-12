{{ config(materialized='table') }}

with agg as (
    select
        stock_ticker,
        min(open_price) as min_open_price,
        max(open_price) as max_open_price, 
        min(close_price) as min_close_price,
        max(close_price) as max_close_price,
        min(trading_volume) as min_trading_volume,
        max(trading_volume) as max_trading_volume
    from {{ ref('stg_stockpricedata') }}
    group by stock_ticker
),

stock_info as (
    select
        stock_ticker,
        min_open_price,
        max_open_price,
        min_close_price,
        max_close_price,
        min_trading_volume,
        max_trading_volume,
        (max_close_price - min_open_price) / nullif(min_open_price, 0) as price_change_percentage,
        (max_close_price * max_trading_volume) as max_market_capitalization,
        (max_close_price * max_trading_volume) / nullif((min_close_price * min_trading_volume), 0) as market_cap_change_percentage
    from agg
)

select *
from stock_info
order by max_trading_volume desc



