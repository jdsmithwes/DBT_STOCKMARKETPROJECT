WITH companydetails AS (

    SELECT
        ticker,
        name,
        description,
        exchange,
        currency,
        sector,
        industry,
        fiscal_year_end,
        latest_quarter,
        market_cap,
        shares_outstanding,
        revenue_ttm            AS revenue,
        gross_profit_ttm       AS gross_profit,
        ebitda,
        pe_ratio               AS pe
    FROM {{ ref('stg_stockoverview') }}

)

SELECT *
FROM companydetails
ORDER BY market_cap DESC





 