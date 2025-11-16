WITH fundamentals AS (
    SELECT
        ticker,
        return_on_equity,
        return_on_assets,
        profit_margin,
        operating_margin_ttm,
        gross_profit_ttm,
        revenue_ttm,
        beta
    FROM {{ ref('stg_stockoverview') }}
),

vol AS (
    SELECT
        ticker,
        daily_volatility
    FROM {{ ref('int_price_momentum') }}
)

SELECT
    f.ticker,
    f.return_on_equity,
    f.return_on_assets,
    f.profit_margin,
    f.operating_margin_ttm,
    f.gross_profit_ttm / NULLIF(f.revenue_ttm, 0) AS gross_margin,

    v.daily_volatility,
    f.beta,

    -- example quality index
    (
        COALESCE(f.return_on_equity, 0) +
        COALESCE(f.return_on_assets, 0) +
        COALESCE(f.profit_margin, 0) +
        COALESCE(f.operating_margin_ttm, 0)
    ) AS quality_score

FROM fundamentals f
LEFT JOIN vol v
    ON f.ticker = v.ticker
