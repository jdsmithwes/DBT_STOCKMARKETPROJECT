with stockdata as (
    select
        TICKER as stock_ticker, 
        "DATE" as trading_date,
        "OPEN" as open_price,
        HIGH as interday_high_price,
        LOW as interday_low_price,
        "CLOSE" as close_price,
        VOLUME as trading_volume
        

        from {{ source('stock_data', 'STOCKDATA_RAW') }}

    )

select * from stockdata