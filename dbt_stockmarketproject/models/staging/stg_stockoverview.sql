{{ config(materialized='view') }}

with source as (

    select
        RAW:"Symbol"::string              as TICKER,
        RAW:"AssetType"::string          as ASSETTYPE,
        RAW:"Name"::string               as NAME,
        RAW:"Description"::string        as DESCRIPTION,
        RAW:"CIK"::string                as CIK,
        RAW:"Exchange"::string           as EXCHANGE,
        RAW:"Currency"::string           as CURRENCY,
        RAW:"Country"::string            as COUNTRY,
        RAW:"Sector"::string             as SECTOR,
        RAW:"Industry"::string           as INDUSTRY,
        RAW:"Address"::string            as ADDRESS,
        RAW:"FiscalYearEnd"::string      as FISCALYEAREND,
        RAW:"LatestQuarter"::string      as LATESTQUARTER,
        RAW:"MarketCapitalization"::string as MARKETCAPITALIZATION,
        RAW:"EBITDA"::string             as EBITDA,
        RAW:"PERatio"::string            as PERATIO,
        RAW:"PEGRatio"::string           as PEGRATIO,
        RAW:"BookValue"::string          as BOOKVALUE,
        RAW:"DividendPerShare"::string   as DIVIDENDPERSHARE,
        RAW:"DividendYield"::string      as DIVIDENDYIELD,
        RAW:"EPS"::string                as EPS,
        RAW:"RevenuePerShareTTM"::string as REVENUEPERSHARETTM,
        RAW:"ProfitMargin"::string       as PROFITMARGIN,
        RAW:"OperatingMarginTTM"::string as OPERATINGMARGINTTM,
        RAW:"ReturnOnAssetsTTM"::string  as RETURNONASSETS,
        RAW:"ReturnOnEquityTTM"::string  as RETURNONEQUITY,
        RAW:"RevenueTTM"::string         as REVENUETTM,
        RAW:"GrossProfitTTM"::string     as GROSSPROFITTTM,
        RAW:"DilutedEPSTTM"::string      as DILUTEDEPSTTM,
        RAW:"QuarterlyEarningsGrowthYOY"::string as QUARTERLYEARNINGSGROWTHYOY,
        RAW:"QuarterlyRevenueGrowthYOY"::string  as QUARTERLYREVENUEGROWTHYOY,
        RAW:"AnalystTargetPrice"::string         as ANALYSTTARGETPRICE,
        RAW:"TrailingPE"::string                 as TRAILINGPE,
        RAW:"ForwardPE"::string                  as FORWARDPE,
        RAW:"PriceToSalesRatioTTM"::string       as PRICETOSALESTTM,
        RAW:"PriceToBookRatio"::string           as PRICETOBOOKRATIO,
        RAW:"EVToRevenue"::string                as EVTOREVENUE,
        RAW:"EVToEBITDA"::string                 as EVTOEBITDA,
        RAW:"Beta"::string                       as BETA,
        RAW:"52WeekHigh"::string                 as WEEK52HIGH,
        RAW:"52WeekLow"::string                  as WEEK52LOW,
        RAW:"50DayMovingAverage"::string         as DAY50MOVINGAVERAGE,
        RAW:"200DayMovingAverage"::string        as DAY200MOVINGAVERAGE,
        RAW:"SharesOutstanding"::string          as SHARESOUTSTANDING,
        RAW:"DividendDate"::string               as DIVIDENDDATE,
        RAW:"ExDividendDate"::string             as EXDIVIDENDDATE,

        METADATA$FILENAME as SOURCE_FILE,
        LOAD_TIME          as LOAD_TIME

    from {{ source('stock_data', 'company_overview_json_raw') }}

)

select * from source