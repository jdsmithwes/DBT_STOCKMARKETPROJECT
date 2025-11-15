import os
import asyncio
import aiohttp
import pandas as pd
import boto3
import logging
import time
from datetime import datetime
from aiohttp import ClientSession, ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed

# ------------------------------------
# Logging Setup
# ------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ------------------------------------
# Environment Variables
# ------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not ALPHAVANTAGE_API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY environment variable.")

S3_KEY = "combined_stock_data.csv"

# ------------------------------------
# S3 Client
# ------------------------------------
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# ------------------------------------
# Fetch S&P 500 Tickers
# ------------------------------------
def fetch_sp500_tickers():
    logging.info("🔍 Fetching S&P 500 tickers…")
    df = pd.read_csv("https://datahub.io/core/s-and-p-500-companies/r/constituents.csv")
    tickers = df["Symbol"].str.upper().tolist()
    logging.info(f"✅ Loaded {len(tickers)} tickers")
    return tickers

# ------------------------------------
# Convert AV JSON → DataFrame
# ------------------------------------
def normalize_alpha_vantage(ticker, ts):
    df = (
        pd.DataFrame.from_dict(ts, orient="index")
        .reset_index()
        .rename(columns={"index": "date"})
    )

    df["ticker"] = ticker

    df.rename(
        columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. adjusted close": "adjusted_close",
            "6. volume": "volume",
            "7. dividend amount": "dividend_amount",
            "8. split coefficient": "split_coefficient",
        },
        inplace=True,
    )

    df["date"] = pd.to_datetime(df["date"])

    numeric_cols = [
        "open", "high", "low", "close",
        "adjusted_close", "volume",
        "dividend_amount", "split_coefficient"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

# ------------------------------------
# Async full-history fetch with retry
# ------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def fetch_full_history(session: ClientSession, ticker: str):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY_ADJUSTED&outputsize=full"
        f"&symbol={ticker}&apikey={ALPHAVANTAGE_API_KEY}"
    )

    async with session.get(url) as resp:
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status}")

        data = await resp.json()

    ts = data.get("Time Series (Daily)")
    if ts is None:
        logging.warning(f"⚠️ {ticker}: No time series in response.")
        return None

    df = normalize_alpha_vantage(ticker, ts)
    logging.info(f"📈 {ticker}: Loaded {df.shape[0]} rows full history")
    return df

# ------------------------------------
# Rate Limiter for Premium (120/min)
# ------------------------------------
API_CALLS_PER_MINUTE = 110
CALL_DELAY = 60 / API_CALLS_PER_MINUTE

# ------------------------------------
# Async driver
# ------------------------------------
async def fetch_all_full_history(tickers):
    timeout = ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=50)

    all_results = []
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for i, ticker in enumerate(tickers):
            logging.info(f"⏳ Fetching {ticker} ({i+1}/{len(tickers)})")

            df = await fetch_full_history(session, ticker)

            if df is not None:
                all_results.append(df)

            await asyncio.sleep(CALL_DELAY)

    return all_results

# ------------------------------------
# Main Workflow
# ------------------------------------
def main():
    logging.info("🚀 Starting FULL historical S&P500 load")

    tickers = fetch_sp500_tickers()

    new_data = asyncio.run(fetch_all_full_history(tickers))

    if not new_data:
        logging.warning("⚠️ No data returned!")
        return

    # FULL REFRESH — NO MERGING — NO COMPARING TO S3
    final_df = pd.concat(new_data, ignore_index=True)

    logging.info(f"📊 Downloaded {final_df.shape[0]} total rows (full history)")

    # Save local
    output_file = "./combined_stock_data.csv"
    final_df.to_csv(output_file, index=False)
    logging.info("💾 Saved dataset locally")

    # Upload to S3
    s3_client.upload_file(output_file, S3_BUCKET_NAME, S3_KEY)
    logging.info(f"🚀 Uploaded dataset → s3://{S3_BUCKET_NAME}/{S3_KEY}")

    logging.info("🎉 Full historical refresh complete!")

# ------------------------------------
# Run
# ------------------------------------
if __name__ == "__main__":
    main()
