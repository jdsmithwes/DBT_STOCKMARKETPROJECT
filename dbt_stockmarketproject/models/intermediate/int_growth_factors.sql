WITH fundamentals AS (
    SELECT
        ticker,
        revenue_ttm,
        gross_profit_ttm,
        eps,
        quarterly_earnings_growth_yoy,
        quarterly_revenue_growth_yoy
    FROM {{ ref('stg_company_overview') }}
),

momentum AS (
    SELECT
        ticker,
        one_month_return,
        three_month_return,
        six_month_return
    FROM {{ ref('int_price_momentum') }}
)

SELECT
    f.ticker,
    f.revenue_ttm,
    f.gross_profit_ttm,
    f.eps,
    f.quarterly_earnings_growth_yoy,
    f.quarterly_revenue_growth_yoy,

    m.one_month_return,
    m.three_month_return,
    m.six_month_return,

    -- combined growth score (example)
    (
        COALESCE(f.quarterly_revenue_growth_yoy, 0) +
        COALESCE(f.quarterly_earnings_growth_yoy, 0) +
        0.5 * COALESCE(m.three_month_return, 0)
    ) AS blended_growth_score

FROM fundamentals f
LEFT JOIN momentum m
    ON f.ticker = m.ticker
