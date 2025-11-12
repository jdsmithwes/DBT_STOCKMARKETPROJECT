with context_info as (
    select
        "Industry",
        "SYMBOL",
        "MarketCapitalization",
        "EBITDA",
        "PERatio",
        "RevenueTTM",
        "GrossProfitTTM",
        "AnalystRatingStrongBuy",
        "AnalystRatingBuy",
        "AnalystRatingHold",
        "AnalystRatingSell",
        "AnalystRatingStrongSell"
    from {{ ref('stg_stockoverview') }}
    )

select
    "Industry",
    count(distinct "SYMBOL") as NumberOfCompanies,
    avg("MarketCapitalization") as AvgMarketCapitalization,
    avg("EBITDA") as AvgEBITDA,
    avg("PERatio") as AvgPERatio,
    avg("RevenueTTM") as AvgRevenueTTM,
    avg("GrossProfitTTM") as AvgGrossProfitTTM,
    count("AnalystRatingStrongBuy") as TotalStrongBuy,
    count("AnalystRatingBuy") as TotalBuy,
    count("AnalystRatingHold") as TotalHold,
    count("AnalystRatingSell") as TotalSell,
    count("AnalystRatingStrongSell") as TotalStrongSell
from context_info
group by "Industry"
order by NumberOfCompanies desc
    
