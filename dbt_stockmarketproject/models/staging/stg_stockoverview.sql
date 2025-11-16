{{ config(materialized='view') }}

with raw as (
    select
        SYMBOL,
        ASSETTYPE,
        NAME,
        DESCRIPTION,
        CIK,
        EXCHANGE,
        CURRENCY,
        COUNTRY,
        SECTOR,
        INDUSTRY,
        ADDRESS,

        -- DATE CASTS
        TRY_TO_DATE(FISCALYEAREND)        as fiscal_year_end,
        TRY_TO_DATE(LATESTQUARTER)        as latest_quarter,

        -- NUMERIC FIELDS
        TRY_TO_NUMBER(MARKETCAPITALIZATION)   as market_cap,
        TRY_TO_NUMBER(EBITDA)                 as ebitda,
        TRY_TO_NUMBER(PERATIO)                as pe_ratio,
        TRY_TO_NUMBER(PEGRATIO)               as peg_ratio,
        TRY_TO_NUMBER(BOOKVALUE)              as book_value,
        TRY_TO_NUMBER(DIVIDENDPERSHARE)       as dividend_per_share,
        TRY_TO_NUMBER(DIVIDENDYIELD)          as dividend_yield,
        TRY_TO_NUMBER(EPS)                    as eps,
        TRY_TO_NUMBER(REVENUEPERSHARETTM)     as revenue_per_share_ttm,
        TRY_TO_NUMBER(PROFITMARGIN)           as profit_margin,
        TRY_TO_NUMBER(OPERATINGMARGINTTM)     as operating_margin_ttm,
        TRY_TO_NUMBER(RETURNONASSETS)         as return_on_assets,
        TRY_TO_NUMBER(RETURNONEQUITY)         as return_on_equity,
        TRY_TO_NUMBER(REVENUETTM)             as revenue_ttm,
        TRY_TO_NUMBER(GROSSPROFITTTM)         as gross_profit_ttm,
        TRY_TO_NUMBER(DILUTEDEPSTTM)          as diluted_eps_ttm,
        TRY_TO_NUMBER(QUARTERLYEARNINGSGROWTHYOY)  as quarterly_earnings_growth_yoy,
        TRY_TO_NUMBER(QUARTERLYREVENUEGROWTHYOY)   as quarterly_revenue_growth_yoy,
        TRY_TO_NUMBER(ANALYSTTARGETPRICE)     as analyst_target_price,
        TRY_TO_NUMBER(TRAILINGPE)             as trailing_pe,
        TRY_TO_NUMBER(FORWARDPE)              as forward_pe,
        TRY_TO_NUMBER(PRICETOSALESTTM)        as price_to_sales_ttm,
        TRY_TO_NUMBER(PRICETOBOOKRATIO)       as price_to_book_ratio,
        TRY_TO_NUMBER(EVTOREVENUE)            as ev_to_revenue,
        TRY_TO_NUMBER(EVTOEBITDA)             as ev_to_ebitda,
        TRY_TO_NUMBER(BETA)                   as beta,
        TRY_TO_NUMBER(WEEK52HIGH)             as week_52_high,
        TRY_TO_NUMBER(WEEK52LOW)              as week_52_low,
        TRY_TO_NUMBER(DAY50MOVINGAVERAGE)     as day_50_ma,
        TRY_TO_NUMBER(DAY200MOVINGAVERAGE)    as day_200_ma,
        TRY_TO_NUMBER(SHARESOUTSTANDING)      as shares_outstanding,

        -- DATES
        TRY_TO_DATE(DIVIDENDDATE)             as dividend_date,
        TRY_TO_DATE(EXDIVIDENDDATE)           as ex_dividend_date,

        -- METADATA
        TICKER,
        INGEST_TIMESTAMP,
        METADATA$FILENAME as source_filename,
        LOAD_TIME,

        TO_DATE(INGEST_TIMESTAMP) as load_date

    from {{ source('stock_data', 'company_overview_raw') }}
)

select * from raw
