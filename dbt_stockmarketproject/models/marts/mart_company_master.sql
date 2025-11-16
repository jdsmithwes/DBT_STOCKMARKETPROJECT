WITH fundamentals AS (
    SELECT *
    FROM {{ ref('int_company_fundamentals') }}
),

momentum AS (
    SELECT *
    FROM {{ ref('int_price_momentum') }}
),

sectors AS (
    SELECT *
    FROM {{ ref('int_sectors') }}
),

growth AS (
    SELECT *
    FROM {{ ref('int_growth_factors') }}
),

quality AS (
    SELECT *
    FROM {{ ref('int_quality_factors') }}
)

SELECT
    f.ticker,
    f.name,
    f.sector,
    f.industry,

    --------------------------
    -- PRICING
    --------------------------
    f.latest_close_price,
    f.volume,
    f.latest_price_date,

    --------------------------
    -- FUNDAMENTALS
    --------------------------
    f.market_cap,
    f.ebitda,
    f.revenue_ttm,
    f.gross_profit_ttm,
    f.pe_ratio,
    f.price_to_sales_ttm,
    f.price_to_book_ratio,
    f.ev_to_ebitda,
    f.ev_to_revenue,
    f.return_on_equity,
    f.return_on_assets,
    f.profit_margin,
    f.operating_margin_ttm,
    f.beta,

    --------------------------
    -- MOMENTUM
    --------------------------
    m.one_month_return,
    m.three_month_return,
    m.six_month_return,
    m.avg_daily_return,
    m.daily_volatility,

    --------------------------
    -- GROWTH
    --------------------------
    g.quarterly_earnings_growth_yoy,
    g.quarterly_revenue_growth_yoy,
    g.blended_growth_score,

    --------------------------
    -- QUALITY
    --------------------------
    q.gross_margin,
    q.quality_score,

    --------------------------
    -- DERIVED RANKS
    --------------------------
    RANK() OVER (ORDER BY f.market_cap DESC) AS market_cap_rank,
    RANK() OVER (ORDER BY m.six_month_return DESC NULLS LAST) AS momentum_rank,
    RANK() OVER (ORDER BY g.blended_growth_score DESC NULLS LAST) AS growth_rank,
    RANK() OVER (ORDER BY q.quality_score DESC NULLS LAST) AS quality_rank

FROM fundamentals f
LEFT JOIN momentum m  ON f.ticker = m.ticker
LEFT JOIN growth g    ON f.ticker = g.ticker
LEFT JOIN quality q   ON f.ticker = q.ticker;
