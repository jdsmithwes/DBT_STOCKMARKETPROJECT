WITH context_info AS (
    SELECT
        INDUSTRY,
        TICKER,
        MARKETCAPITALIZATION,
        EBITDA,
        PERATIO,
        REVENUETTM,
        GROSSPROFITTTM
    FROM {{ ref('stg_stockoverview') }}
)

SELECT
    INDUSTRY,
    COUNT(DISTINCT TICKER)           AS number_of_companies,
    AVG(MARKETCAPITALIZATION)        AS avg_market_cap,
    AVG(EBITDA)                      AS avg_ebitda,
    AVG(PERATIO)                     AS avg_pe_ratio,
    AVG(REVENUETTM)                  AS avg_revenue_ttm,
    AVG(GROSSPROFITTTM)              AS avg_gross_profit_ttm
FROM context_info
GROUP BY INDUSTRY
ORDER BY number_of_companies DESC;
