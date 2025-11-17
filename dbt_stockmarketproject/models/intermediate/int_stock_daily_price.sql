WITH base AS (

    SELECT
        DATE                    AS trading_date,
        TICKER                  AS stock_ticker,
        OPEN                    AS open_price,
        HIGH                    AS interday_high_price,
        LOW                     AS interday_low_price,
        CLOSE                   AS close_price,
        ADJ_CLOSE               AS adjusted_close_price,
        VOLUME                  AS trading_volume,
        DIVIDEND                AS dividend_amount,
        SPLIT                   AS split_coefficient
    FROM {{ ref('stg_stockpricedata') }}

),

summary AS (

    SELECT
        stock_ticker,

        MIN(trading_date)            AS first_trading_date,
        MAX(trading_date)            AS last_trading_date,

        AVG(close_price)             AS avg_close_price,
        MAX(interday_high_price)     AS all_time_high,
        MIN(interday_low_price)      AS all_time_low,

        SUM(trading_volume)          AS total_volume_traded,
        MAX(trading_volume)          AS max_daily_volume,

        SUM(dividend_amount)         AS total_dividends

    FROM base
    GROUP BY stock_ticker
)

SELECT *
FROM summary