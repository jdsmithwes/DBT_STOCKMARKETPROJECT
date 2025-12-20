{{ config(materialized='table') }}

with base as (

    select
        p.TICKER,
        p.TRADING_DATE,
        p.CLOSE_PRICE,
        p.VOLUME
    from {{ ref('stg_stockpricedata') }} p

),

stats as (

    select
        b.TICKER,
        b.TRADING_DATE,
        b.CLOSE_PRICE,
        b.VOLUME,

        -- previous close price
        lag(b.CLOSE_PRICE)
            over (
                partition by b.TICKER
                order by b.TRADING_DATE
            ) as PREV_CLOSE_PRICE,

        -- daily return (Snowflake-safe)
        (
            to_double(b.CLOSE_PRICE)
            /
            nullif(
                to_double(
                    lag(b.CLOSE_PRICE)
                        over (
                            partition by b.TICKER
                            order by b.TRADING_DATE
                        )
                ),
                0
            )
        ) - 1 as DAILY_RETURN

    from base b

)

select *
from stats