{{ config(
    materialized = 'view',
    schema = 'STAGING'
) }}

WITH company_overview AS (

    SELECT
        COL1  AS Symbol,
        COL2  AS AssetType,
        COL3  AS "Name",
        COL4  AS "Description",
        COL5  AS "CIK",
        COL6  AS "Exchange",
        COL7  AS "Currency",
        COL8  AS "Country",
        COL9  AS "Sector",
        COL10 AS "Industry",
        COL11 AS "Address",
        COL12 AS "OfficialSite",
        COL13 AS "FiscalYearEnd",
        TRY_TO_DATE(NULLIF(UPPER(COL14), 'NONE'), 'YYYY-MM-DD') AS "LatestQuarter",
        TRY_TO_NUMBER(NULLIF(UPPER(COL15), 'NONE')) AS "MarketCapitalization",
        TRY_TO_NUMBER(NULLIF(UPPER(COL16), 'NONE')) AS "EBITDA",
        TRY_TO_NUMBER(NULLIF(UPPER(COL17), 'NONE')) AS "PERatio",
        TRY_TO_NUMBER(NULLIF(UPPER(COL18), 'NONE')) AS "PEGRatio",
        TRY_TO_NUMBER(NULLIF(UPPER(COL19), 'NONE')) AS "BookValue",
        TRY_TO_NUMBER(NULLIF(UPPER(COL20), 'NONE')) AS "DividendPerShare",
        TRY_TO_NUMBER(NULLIF(UPPER(COL21), 'NONE')) AS "DividendYield",
        TRY_TO_NUMBER(NULLIF(UPPER(COL22), 'NONE')) AS "EPS",
        TRY_TO_NUMBER(NULLIF(UPPER(COL23), 'NONE')) AS "RevenuePerShareTTM",
        TRY_TO_NUMBER(NULLIF(UPPER(COL24), 'NONE')) AS "ProfitMargin",
        TRY_TO_NUMBER(NULLIF(UPPER(COL25), 'NONE')) AS "OperatingMarginTTM",
        TRY_TO_NUMBER(NULLIF(UPPER(COL26), 'NONE')) AS "ReturnOnAssetsTTM",
        TRY_TO_NUMBER(NULLIF(UPPER(COL27), 'NONE')) AS "ReturnOnEquityTTM",
        TRY_TO_NUMBER(NULLIF(UPPER(COL28), 'NONE')) AS "RevenueTTM",
        TRY_TO_NUMBER(NULLIF(UPPER(COL29), 'NONE')) AS "GrossProfitTTM",
        TRY_TO_NUMBER(NULLIF(UPPER(COL30), 'NONE')) AS "DilutedEPSTTM",
        TRY_TO_NUMBER(NULLIF(UPPER(COL31), 'NONE')) AS "QuarterlyEarningsGrowthYOY",
        TRY_TO_NUMBER(NULLIF(UPPER(COL32), 'NONE')) AS "QuarterlyRevenueGrowthYOY",
        TRY_TO_NUMBER(NULLIF(UPPER(COL33), 'NONE')) AS "AnalystTargetPrice",
        TRY_TO_NUMBER(NULLIF(UPPER(COL34), 'NONE')) AS "AnalystRatingStrongBuy",
        TRY_TO_NUMBER(NULLIF(UPPER(COL35), 'NONE')) AS "AnalystRatingBuy",
        TRY_TO_NUMBER(NULLIF(UPPER(COL36), 'NONE')) AS "AnalystRatingHold",
        TRY_TO_NUMBER(NULLIF(UPPER(COL37), 'NONE')) AS "AnalystRatingSell",
        TRY_TO_NUMBER(NULLIF(UPPER(COL38), 'NONE')) AS "AnalystRatingStrongSell",
        TRY_TO_NUMBER(NULLIF(UPPER(COL39), 'NONE')) AS "TrailingPE",
        TRY_TO_NUMBER(NULLIF(UPPER(COL40), 'NONE')) AS "ForwardPE",
        TRY_TO_NUMBER(NULLIF(UPPER(COL41), 'NONE')) AS "PriceToSalesRatioTTM",
        TRY_TO_NUMBER(NULLIF(UPPER(COL42), 'NONE')) AS "PriceToBookRatioTTM",
        TRY_TO_NUMBER(NULLIF(UPPER(COL43), 'NONE')) AS "EVToRevenue",
        TRY_TO_NUMBER(NULLIF(UPPER(COL44), 'NONE')) AS "EVToEBITDA",
        TRY_TO_NUMBER(NULLIF(UPPER(COL45), 'NONE')) AS "Beta",
        TRY_TO_NUMBER(NULLIF(UPPER(COL46), 'NONE')) AS "52WeekHigh",
        TRY_TO_NUMBER(NULLIF(UPPER(COL47), 'NONE')) AS "52WeekLow",
        TRY_TO_NUMBER(NULLIF(UPPER(COL48), 'NONE')) AS "50DayMovingAverage",
        TRY_TO_NUMBER(NULLIF(UPPER(COL49), 'NONE')) AS "200DayMovingAverage",
        TRY_TO_NUMBER(NULLIF(UPPER(COL50), 'NONE')) AS "SharesOutstanding",
        TRY_TO_NUMBER(NULLIF(UPPER(COL51), 'NONE')) AS "SharesFloat",
        TRY_TO_NUMBER(NULLIF(UPPER(COL52), 'NONE')) AS "PercentInsiders",
        TRY_TO_NUMBER(NULLIF(UPPER(COL53), 'NONE')) AS "PercentInstitutions",
        TRY_TO_DATE(NULLIF(UPPER(COL54), 'NONE'), 'YYYY-MM-DD') AS "DividendDate",
        TRY_TO_DATE(NULLIF(UPPER(COL55), 'NONE'), 'YYYY-MM-DD') AS "ExDividendDate",
        COL56 AS "Ticker"
    FROM {{ source('stock_data', 'COMPANY_OVERVIEW_RAW') }}

)

SELECT *
FROM company_overview



