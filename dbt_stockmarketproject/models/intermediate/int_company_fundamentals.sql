WITH fundamentals AS (
    SELECT
        ticker,
        name,
        sector,
        industry,
        market_cap,
        ebitda,
        revenue_ttm,
        gross_profit_ttm,
        pe_ratio,
        price_to_sales_ttm,
        price_to_book_ratio,
        ev_to_ebitda,
        ev_to_revenue,
        return_on_equity,
        return_on_assets,
        profit_margin,
        operating_margin_ttm,
        beta
    FROM {{ ref('stg_stockoverview') }}
),

latest_price AS (
    SELECT
        ticker,
        close AS latest_close_price,
        volume,
        date_trade
    FROM {{ ref('stg_stockpricedata') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date_trade DESC) = 1
)

SELECT
    f.ticker,
    f.name,
    f.sector,
    f.industry,
    l.latest_close_price,

    -- valuation ratios
    f.market_cap,
    f.ebitda,
    f.revenue_ttm,
    f.gross_profit_ttm,
    f.pe_ratio,
    f.price_to_sales_ttm,
    f.price_to_book_ratio,
    f.ev_to_ebitda,
    f.ev_to_revenue,

    -- profitability
    f.return_on_equity,
    f.return_on_assets,
    f.profit_margin,
    f.operating_margin_ttm,

    -- misc
    f.beta,
    l.volume,
    l.date_trade AS latest_price_date,

    -- derived valuations
    CASE WHEN f.eps IS NOT NULL THEN l.latest_close_price / f.eps END AS price_to_earnings,
    CASE WHEN f.revenue_ttm IS NOT NULL AND f.revenue_ttm != 0 
         THEN l.latest_close_price / (f.revenue_ttm / f.shares_outstanding)
    END AS price_to_sales_latest,
    CASE WHEN f.gross_profit_ttm IS NOT NULL THEN f.gross_profit_ttm / f.revenue_ttm END AS gross_margin

FROM fundamentals f
LEFT JOIN latest_price l
    ON f.ticker = l.ticker