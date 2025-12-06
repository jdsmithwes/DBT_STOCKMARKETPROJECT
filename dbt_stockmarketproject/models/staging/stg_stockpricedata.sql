{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['stock_ticker', 'trading_date']
) }}

with stockdata as (
    select
        TICKER as stock_ticker, 
        "DATE" as trading_date,
        "OPEN" as open_price,
        HIGH as interday_high_price,
        LOW as interday_low_price,
        "CLOSE" as close_price,
        VOLUME as trading_volume,
        DIVIDEND_AMOUNT as dividend_amount,
        SPLIT_COEFFICIENT as split_coefficient,
        ADJUSTED_CLOSE as adjusted_close_price,
        LOAD_TIME
    from {{ source('stock_data', 'stock_price_data_raw') }}

    {% if is_incremental() %}
        -- Load only rows that arrived after the latest load_time already processed
        where LOAD_TIME > (select max(LOAD_TIME) from {{ this }})
    {% endif %}
)

select
    stock_ticker,
    trading_date,
    open_price,
    interday_high_price,
    interday_low_price,
    close_price,
    trading_volume,
    dividend_amount,
    split_coefficient,
    adjusted_close_price
from stockdata
