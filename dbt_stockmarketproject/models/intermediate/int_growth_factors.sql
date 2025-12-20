{{ config(materialized='view') }}

with base as (

    select
        TICKER,
        NAME,
        SECTOR,
        INDUSTRY,

        to_double(REVENUETTM)                    as revenue_ttm,
        to_double(GROSSPROFITTTM)                as gross_profit_ttm,
        to_double(QUARTERLYEARNINGSGROWTHYOY)    as earnings_growth_yoy,
        to_double(QUARTERLYREVENUEGROWTHYOY)     as revenue_growth_yoy,
        to_double(REVENUEPERSHARETTM)            as revenue_per_share_ttm,
        to_double(EPS)                           as eps,
        to_double(DILUTEDEPSTTM)                 as diluted_eps_ttm,

        LOAD_TIME
    from {{ ref('stg_stockoverview') }}
),

growth_factors as (

    select
        *,

        /* 1. Normalize growth metrics into multipliers */
        1 + (to_double(revenue_growth_yoy) / 100)   as revenue_growth_factor,
        1 + (to_double(earnings_growth_yoy) / 100)  as earnings_growth_factor,

        /* 2. Revenue per share and EPS ratios */
        nullif(to_double(revenue_per_share_ttm), 0) as revenue_per_share,
        nullif(to_double(eps), 0)                    as eps_val,
        nullif(to_double(diluted_eps_ttm), 0)        as diluted_eps_val,

        /* 3. EPS growth signal (derived) */
        case
            when eps_val is not null
             and diluted_eps_val is not null
             and diluted_eps_val <> 0
            then to_double(eps_val) / to_double(diluted_eps_val)
        end as eps_growth_factor,

        /* 4. Blended growth rate (simple average, % based) */
        (
            coalesce(to_double(revenue_growth_yoy), 0)
          + coalesce(to_double(earnings_growth_yoy), 0)
        ) / 2.0 as blended_growth_rate_simple,

        /* 5. Blended growth factor (multiplicative, SAFE) */
        (
            coalesce(1 + to_double(revenue_growth_yoy) / 100, 1)
          * coalesce(1 + to_double(earnings_growth_yoy) / 100, 1)
        ) - 1 as blended_growth_factor_mult,

        /* 6. Full weighted blended score */
        (
              0.4 * coalesce(to_double(revenue_growth_yoy), 0)
            + 0.4 * coalesce(to_double(earnings_growth_yoy), 0)
            + 0.2 * coalesce(
                    to_double(eps_growth_factor) * 100 - 100,
                    0
                  )
        ) as blended_growth_score_weighted

    from base
)

select *
from growth_factors
