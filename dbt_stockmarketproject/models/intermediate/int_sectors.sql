WITH context_info AS (

    SELECT
        industry,
        ticker,
        market_cap,
        ebitda,
        pe_ratio,
        revenue_ttm,
        gross_profit_ttm
    FROM {{ ref('stg_stockoverview') }}

)

SELECT
    industry,
    COUNT(DISTINCT ticker)                       AS number_of_companies,
    AVG(market_cap)                              AS avg_market_cap,
    AVG(ebitda)                                   AS avg_ebitda,
    AVG(pe_ratio)                                 AS avg_pe_ratio,
    AVG(revenue_ttm)                              AS avg_revenue_ttm,
    AVG(gross_profit_ttm)                         AS avg_gross_profit_ttm
FROM context_info
GROUP BY industry
ORDER BY number_of_companies DESC

