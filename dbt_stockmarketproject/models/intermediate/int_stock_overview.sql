with companydetails as (
    select
        "Ticker",
        "Name",
        "Description",
        "Exchange",
        "Currency",
        "Sector",
        "Industry",
        "FiscalYearEnd",
        "LatestQuarter",
        "MarketCapitalization",
        "SharesOutstanding",
        "RevenueTTM" as Revenue,
        "GrossProfitTTM" as GrossProfit,
        "EBITDA",
        "PERatio" as PE
    from {{ ref('stg_stockoverview') }}
)

select * 
from companydetails
order by "MarketCapitalization" desc




 