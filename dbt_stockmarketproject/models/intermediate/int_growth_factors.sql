{{ config(materialized='view') }}

select
    TICKER,
    NAME,
    SECTOR,
    INDUSTRY,
    REVENUETTM,
    GROSSPROFITTTM,
    QUARTERLYEARNINGSGROWTHYOY,
    QUARTERLYREVENUEGROWTHYOY,
    REVENUEPERSHARETTM,
    EPS,
    DILUTEDEPSTTM,
    LOAD_TIME

from {{ ref('stg_stockoverview') }}