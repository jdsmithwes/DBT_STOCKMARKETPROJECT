WITH fundamentals AS (

    SELECT
        TICKER,
        NAME,
        ASSETTYPE,
        DESCRIPTION,
        CIK,
        EXCHANGE,
        CURRENCY,
        COUNTRY,
        SECTOR,
        INDUSTRY,
        ADDRESS,
        FISCALYEAREND,
        LATESTQUARTER,
        MARKETCAPITALIZATION,
        EBITDA,
        PERATIO,
        PEGRATIO,
        BOOKVALUE,
        DIVIDENDPERSHARE,
        DIVIDENDYIELD,
        EPS,
        REVENUEPERSHARETTM,
        PROFITMARGIN,
        OPERATINGMARGINTTM,
        RETURNONASSETS,
        RETURNONEQUITY,
        REVENUETTM,
        GROSSPROFITTTM,
        DILUTEDEPSTTM,
        QUARTERLYEARNINGSGROWTHYOY,
        QUARTERLYREVENUEGROWTHYOY,
        ANALYSTTARGETPRICE,
        TRAILINGPE,
        FORWARDPE,
        PRICETOSALESTTM,
        PRICETOBOOKRATIO,
        EVTOREVENUE,
        EVTOEBITDA,
        BETA,
        WEEK52HIGH,
        WEEK52LOW,
        DAY50MOVINGAVERAGE,
        DAY200MOVINGAVERAGE,
        SHARESOUTSTANDING,
        DIVIDENDDATE,
        EXDIVIDENDDATE,
        SOURCE_FILE,
        INGEST_TIMESTAMP
    FROM {{ ref('stg_stockoverview') }}

),

latest_price AS (

    SELECT
        stock_ticker AS ticker,
        close_price,
        trading_volume,
        trading_date
    FROM {{ ref('stg_stockpricedata') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY stock_ticker ORDER BY trading_date DESC
    ) = 1
)

SELECT
    f.*,
    lp.close_price      AS latest_close_price,
    lp.trading_volume   AS latest_volume,
    lp.trading_date     AS latest_price_date

FROM fundamentals f
LEFT JOIN latest_price lp
    ON f.ticker = lp.ticker