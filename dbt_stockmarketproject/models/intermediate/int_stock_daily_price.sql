{{ config(materialized='table') }}

WITH base AS (

    SELECT
        trading_date,
        stock_ticker,
        open_price,
        high_price AS interday_high_price,
        low_price  AS interday_low_price,
        close_price,
        adjusted_close_price,
        trading_volume,
        dividend_amount,
        split_coefficient
    FROM {{ ref('stg_stockpricedata') }}

),

summary AS (

    SELECT
        stock_ticker,

        -- first and last trading days
        MIN(trading_date) AS first_trading_date,
        MAX(trading_date) AS last_trading_date,

        -- prices
        AVG(close_price) AS avg_close_price,
        MAX(interday_high_price) AS all_time_high,
        MIN(interday_low_price)  AS all_time_low,

        -- volume
        SUM(trading_volume) AS total_volume_traded,
        MAX(trading_volume) AS max_daily_volume,

        -- dividends
        SUM(dividend_amount) AS total_dividends

    FROM base
    GROUP BY stock_ticker
)

SELECT * FROM summary;
