WITH daily_prices AS (
    SELECT
        ticker,
        date_trade,
        close,
        LAG(close) OVER (PARTITION BY ticker ORDER BY date_trade) AS prev_close
    FROM {{ ref('stg_stockpricedata') }}
),

returns AS (
    SELECT
        ticker,
        date_trade,
        close,
        (close - prev_close) / prev_close AS daily_return
    FROM daily_prices
    WHERE prev_close IS NOT NULL
),

aggregates AS (
    SELECT
        ticker,
        AVG(daily_return) AS avg_daily_return,
        STDDEV(daily_return) AS daily_volatility,

        -- momentum windows
        SUM(daily_return) FILTER (WHERE date_trade >= DATEADD(month, -1, CURRENT_DATE)) AS one_month_return,
        SUM(daily_return) FILTER (WHERE date_trade >= DATEADD(month, -3, CURRENT_DATE)) AS three_month_return,
        SUM(daily_return) FILTER (WHERE date_trade >= DATEADD(month, -6, CURRENT_DATE)) AS six_month_return
    FROM returns
    GROUP BY ticker
)

SELECT *
FROM aggregates