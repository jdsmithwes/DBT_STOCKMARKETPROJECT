# Script generated with the assistance of my dear friend Perplexity

import requests
import pandas as pd

API_KEY = 'hov7uoZ9SaA5_fmCIuIcvIeApPInviN3'
BASE_URL = 'https://api.massive.com/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}'

def fetch_massive_historical(ticker, start_date, end_date, timespan="day", multiplier=1):
    url = BASE_URL.format(
        ticker=ticker,
        multiplier=multiplier,
        timespan=timespan,
        start_date=start_date,
        end_date=end_date
    )
    params = {'apiKey': API_KEY}
    response = requests.get(url, params=params)
    if response.ok:
        data = response.json()['results']
        return pd.DataFrame(data)
    else:
        print(f"Error for {ticker}: ", response.status_code, response.text)
        return None

tickers = ['AAPL', 'MSFT', 'GOOG']
start_date = '2023-01-01'
end_date = '2023-12-31'

frames = {}
for ticker in tickers:
    df = fetch_massive_historical(ticker, start_date, end_date)
    if df is not None:
        frames[ticker] = df

# Each ticker's DataFrame is now in frames, keyed by ticker symbol
# For example, frames['AAPL'] gives Apple's historical prices

