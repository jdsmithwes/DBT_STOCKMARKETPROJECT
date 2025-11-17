{{ config(materialized='view') }}

select
    TICKER,
    REVENUE_TTM,
    GROSS_PROFIT_TTM,
    QUARTERLY_EARNINGS_GROWTH_YOY,
    QUARTERLY_REVENUE_GROWTH_YOY,
    REVENUE_PER_SHARE_TTM,
    DILUTED_EPS_TTM,
    LOAD_TIME

from {{ ref('stg_stockoverview') }}