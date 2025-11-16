
with stockdata as (
    select
        TICKER as stock_ticker, 
        "DATE" as trading_date,
        "OPEN" as open_price,
        HIGH as interday_high_price,
        LOW as interday_low_price,
        "CLOSE" as close_price,
        VOLUME as trading_volume,
        DIVIDEND_AMOUNT as dividend_amount,
        SPLIT_COEFFICIENT as split_coefficient,
        ADJUSTED_CLOSE as adjusted_close_price
        

        from {{ source('stock_data', 'stock_price_data_raw') }}

    )

select * from stockdata