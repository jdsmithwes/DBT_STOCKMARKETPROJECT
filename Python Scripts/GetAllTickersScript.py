# Thank you Perplexity for helping me be great,lol.

import requests

API_KEY = 'hov7uoZ9SaA5_fmCIuIcvIeApPInviN3'
BASE_URL = 'https://api.massive.com/v3/reference/tickers'

def get_all_tickers(exchanges=['NYSE', 'NASDAQ']):
    tickers = []
    for exchange in exchanges:
        params = {
            'exchange': exchange,  # Specify NYSE or NASDAQ
            'active': 'true',      # Only active symbols
            'limit': 1000,         # Can page through for more
            'apiKey': API_KEY
        }
        page = 1
        while True:
            params['page'] = page
            response = requests.get(BASE_URL, params=params)
            if response.ok:
                data = response.json()
                for entry in data.get('results', []):
                    tickers.append(entry['ticker'])
                if not data.get('next_page_token'):  # No more pages
                    break
                page += 1
            else:
                print(f"Error on {exchange} page {page}: {response.text}")
                break
    return tickers

# Example usage:
nyse_nasdaq_tickers = get_all_tickers()
print(f"Tickers found: {len(nyse_nasdaq_tickers)}")
print(nyse_nasdaq_tickers[:10])  # Print first 10 as a sample
