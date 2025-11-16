WITH source_data AS (

    SELECT
        -- base alpha vantage fields
        symbol,
        assettype,
        name,
        description,
        cik,
        exchange,
        currency,
        country,
        sector,
        industry,
        address,

        -- date strings -> cast to DATE
        TRY_TO_DATE(fiscalyearend)         AS fiscal_year_end,
        TRY_TO_DATE(latestquarter)         AS latest_quarter,

        -- numeric casts
        TRY_TO_NUMBER(marketcapitalization)    AS market_cap,
        TRY_TO_NUMBER(ebitda)                  AS ebitda,
        TRY_TO_NUMBER(peratio)                 AS pe_ratio,
        TRY_TO_NUMBER(pegratio)                AS peg_ratio,
        TRY_TO_NUMBER(bookvalue)               AS book_value,
        TRY_TO_NUMBER(dividendpershare)        AS dividend_per_share,
        TRY_TO_NUMBER(dividendyield)           AS dividend_yield,
        TRY_TO_NUMBER(eps)                     AS eps,
        TRY_TO_NUMBER(revenuepersharettm)      AS revenue_per_share_ttm,
        TRY_TO_NUMBER(profitmargin)            AS profit_margin,
        TRY_TO_NUMBER(operatingmarginttm)      AS operating_margin_ttm,
        TRY_TO_NUMBER(returnonassets)          AS return_on_assets,
        TRY_TO_NUMBER(returnonequity)          AS return_on_equity,
        TRY_TO_NUMBER(revenueTTM)              AS revenue_ttm,
        TRY_TO_NUMBER(grossProfitTTM)          AS gross_profit_ttm,
        TRY_TO_NUMBER(dilutedEPSTTM)           AS diluted_eps_ttm,
        TRY_TO_NUMBER(quarterlyEarningsGrowthYOY)    AS quarterly_earnings_growth_yoy,
        TRY_TO_NUMBER(quarterlyRevenueGrowthYOY)     AS quarterly_revenue_growth_yoy,
        TRY_TO_NUMBER(analystTargetPrice)      AS analyst_target_price,
        TRY_TO_NUMBER(trailingPE)              AS trailing_pe,
        TRY_TO_NUMBER(forwardPE)               AS forward_pe,
        TRY_TO_NUMBER(priceToSalesTTM)         AS price_to_sales_ttm,
        TRY_TO_NUMBER(priceToBookRatio)        AS price_to_book_ratio,
        TRY_TO_NUMBER(evToRevenue)             AS ev_to_revenue,
        TRY_TO_NUMBER(evToEBITDA)              AS ev_to_ebitda,
        TRY_TO_NUMBER(beta)                    AS beta,
        TRY_TO_NUMBER(week52high)              AS week_52_high,
        TRY_TO_NUMBER(week52low)               AS week_52_low,
        TRY_TO_NUMBER(day50movingaverage)      AS day_50_ma,
        TRY_TO_NUMBER(day200movingaverage)     AS day_200_ma,
        TRY_TO_NUMBER(sharesoutstanding)       AS shares_outstanding,

        TRY_TO_DATE(dividenddate)              AS dividend_date,
        TRY_TO_DATE(exdividenddate)            AS ex_dividend_date,

        -- script + pipeline metadata
        ticker,
        ingest_timestamp,
        metadata$filename                    AS source_filename,
        load_time,

        -- load date (for partitioning / snapshots)
        TO_DATE(ingest_timestamp)             AS load_date

    FROM {{ source('stock_data', 'company_overview_raw') }}

)

SELECT
    *
FROM source_data
