{{
    config(
        materialized = 'incremental',
        unique_key = ['TICKER', 'TRADING_DATE']
    )
}}

with raw as (

    select
        r.TICKER                                           as TICKER,

        -- explicit date casting (prevents dbt compile-time string math)
        to_date(r.DATE)                                    as TRADING_DATE,

        -- canonical price fields
        to_double(r.OPEN)                                  as OPEN_PRICE,
        to_double(r.HIGH)                                  as HIGH_PRICE,
        to_double(r.LOW)                                   as LOW_PRICE,
        to_double(r.CLOSE)                                 as CLOSE_PRICE,
        to_double(r.ADJUSTED_CLOSE)                         as ADJUSTED_CLOSE_PRICE,

        to_number(r.VOLUME)                                as VOLUME,
        to_double(r.DIVIDEND_AMOUNT)                        as DIVIDEND_AMOUNT,
        to_double(r.SPLIT_COEFFICIENT)                      as SPLIT_COEFFICIENT,

        -- explicit timestamp casting
        to_timestamp_ntz(r.LOAD_TIME)                      as LOAD_TIME

    from {{ source('stock_data', 'stock_price_data_raw') }} r

    {% if is_incremental() %}
        where to_timestamp_ntz(r.LOAD_TIME) >
            (
                select coalesce(
                    max(t.LOAD_TIME),
                    to_timestamp_ntz('1900-01-01')
                )
                from {{ this }} t
            )
    {% endif %}

)

select *
from raw
where TICKER is not null
