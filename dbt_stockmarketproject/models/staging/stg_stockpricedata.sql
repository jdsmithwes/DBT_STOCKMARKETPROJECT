{{ config(
    materialized='incremental',
    unique_key='trading_date || \'-\' || ticker',
    incremental_strategy='merge'
) }}

WITH raw AS (

    SELECT
        DATE                AS trading_date,
        OPEN                AS open_price,
        HIGH                AS interday_high_price,
        LOW                 AS interday_low_price,
        CLOSE               AS close_price,
        ADJUSTED_CLOSE      AS adjusted_close_price,
        VOLUME              AS trading_volume,
        DIVIDEND_AMOUNT     AS dividend_amount,
        SPLIT_COEFFICIENT   AS split_coefficient,
        TICKER              AS ticker,
        LOAD_TIME
    FROM {{ source('stock_data','stock_price_data_raw') }}

),

most_recent AS (
    SELECT MAX(load_time) AS max_load_time
    FROM raw
),

filtered AS (
    SELECT r.*
    FROM raw r
    JOIN most_recent m
        ON r.load_time = m.max_load_time
)

{% if is_incremental() %}
, incremental_filtered AS (
    SELECT *
    FROM filtered
    WHERE trading_date > (
        SELECT COALESCE(MAX(trading_date), '1900-01-01')
        FROM {{ this }}
    )
)
{% endif %}

SELECT *
FROM {% if is_incremental() %} incremental_filtered {% else %} filtered {% endif %}

