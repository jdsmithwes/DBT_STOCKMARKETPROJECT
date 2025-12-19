{{ config(
    materialized='incremental',
    unique_key='trading_date || '-' || ticker',
    incremental_strategy='merge'
) }}

-- 1. Pull all raw rows
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

-- 2. Identify the latest load batch in the RAW table
latest_batch AS (
    SELECT MAX(load_time) AS max_load_time
    FROM raw
),

-- 3. Select only rows from the most recent batch
current_batch AS (
    SELECT r.*
    FROM raw r
    JOIN latest_batch b
      ON r.load_time = b.max_load_time
)

{% if is_incremental() %}

-- 4. When incrementally refreshing, ONLY load new load batches
, incremental_rows AS (
    SELECT *
    FROM current_batch
    WHERE load_time > (
        SELECT COALESCE(MAX(load_time), '1900-01-01')
        FROM {{ this }}
    )
)

{% endif %}

-- 5. Final select
SELECT *
FROM {% if is_incremental() %} incremental_rows {% else %} current_batch {% endif %}
