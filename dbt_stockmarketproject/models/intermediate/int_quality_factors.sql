WITH fundamentals AS (
    SELECT
        TICKER,
        RETURNONEQUITY,
        RETURNONASSETS,
        PROFITMARGIN,
        OPERATINGMARGINTTM,
        GROSSPROFITTTM,
        REVENUETTM,
        BETA
    FROM {{ ref('stg_stockoverview') }}
),

vol AS (
    SELECT
        stock_ticker,
        daily_volatility
    FROM {{ ref('int_price_momentum') }}
)

SELECT
    f.TICKER,
    f.RETURNONEQUITY                  AS return_on_equity,
    f.RETURNONASSETS                  AS return_on_assets,
    f.PROFITMARGIN                    AS profit_margin,
    f.OPERATINGMARGINTTM              AS operating_margin_ttm,

    -- Gross margin safely calculated
    f.GROSSPROFITTTM / NULLIF(f.REVENUETTM, 0) AS gross_margin,

    v.daily_volatility,
    f.BETA,

    -- Unified quality score
    (
        COALESCE(f.RETURNONEQUITY, 0) +
        COALESCE(f.RETURNONASSETS, 0) +
        COALESCE(f.PROFITMARGIN, 0) +
        COALESCE(f.OPERATINGMARGINTTM, 0)
    ) AS quality_score

FROM fundamentals f
LEFT JOIN vol v
    ON f.TICKER = v.stock_ticker