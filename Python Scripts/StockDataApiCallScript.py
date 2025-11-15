import os
import asyncio
import aiohttp
import pandas as pd
import boto3
import logging
from datetime import datetime
from io import StringIO
from aiohttp import ClientSession, ClientTimeout

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

# ------------------------------------
# S3 Client
# ------------------------------------
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

S3_KEY = "combined_stock_data.csv"

# ------------------------------------
# Load Existing S3 Data (Incremental Logic)
# ------------------------------------
def load_existing_s3_data():
    try:
        csv_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY)
        existing_df = pd.read_csv(csv_obj["Body"])
        logging.info(f"📄 Loaded existing dataset: {existing_df.shape[0]} rows")

        most_recent_date = existing_df["date"].max()
        logging.info(f"📅 Most recent date in S3: {most_recent_date}")

        return existing_df, most_recent_date

    except s3_client.exceptions.NoSuchKey:
        logging.info("ℹ️ No existing S3 file found. Creating new dataset.")
        return pd.DataFrame(), None

# ------------------------------------
# Fetch S&P500 Ticklers
# ------------------------------------
def fetch_sp500_tickers():
    logging.info("🔍 Fetching S&P 500 tickers from DataHub…")
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    df = pd.read_csv(url)
    tickers = df["Symbol"].str.upper().tolist()
    logging.info(f"✅ Loaded {len(tickers)} tickers")
    return tickers

# ------------------------------------
# Async Fetch NEW Alpha Vantage Data
# ------------------------------------
async def fetch_new_daily_data(session: ClientSession, ticker: str, most_recent_date):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={ALPHAVANTAGE_API_KEY}"
    )

    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logging.warning(f"⚠️ {ticker}: HTTP {resp.status}")
                return None

            data = await resp.json()
    except Exception as e:
        logging.warning(f"⚠️ {ticker}: API error {e}")
        return None

    ts = data.get("Time Series (Daily)")
    if ts is None:
        return None

    df = pd.DataFrame.from_dict(ts, orient="index")
    df.index.name = "date"
    df.reset_index(inplace=True)
    df["ticker"] = ticker

    # Only new rows if existing file present
    if most_recent_date:
        df = df[df["date"] > most_recent_date]

    if df.empty:
        return None

    logging.info(f"📈 {ticker}: {df.shape[0]} new rows")
    return df

# ------------------------------------
# Async Driver
# ------------------------------------
async def fetch_all_new_data(tickers, most_recent_date):
    timeout = ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=50)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [
            fetch_new_daily_data(session, ticker, most_recent_date)
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks)

    return [df for df in results if df is not None]

# ------------------------------------
# Main Workflow
# ------------------------------------
def main():
    logging.info("🚀 Starting incremental S&P500 data load")

    existing_df, most_recent_date = load_existing_s3_data()
    tickers = fetch_sp500_tickers()

    new_data = asyncio.run(fetch_all_new_data(tickers, most_recent_date))

    if not new_data:
        logging.info("✅ No new data found for any ticker. Exiting.")
        return

    new_df = pd.concat(new_data)
    logging.info(f"📊 Total new rows: {new_df.shape[0]}")

    # Append new rows to existing dataset
    final_df = (
        pd.concat([existing_df, new_df], ignore_index=True)
        if not existing_df.empty
        else new_df
    )

    # Save locally
    output_file = "./combined_stock_data.csv"
    final_df.to_csv(output_file, index=False)
    logging.info("💾 Saved updated dataset locally")

    # Upload to S3
    s3_client.upload_file(output_file, S3_BUCKET_NAME, S3_KEY)

    logging.info(f"🚀 Uploaded updated dataset → s3://{S3_BUCKET_NAME}/{S3_KEY}")
    logging.info("🎉 Incremental update complete!")

# ------------------------------------
# Run
# ------------------------------------
if __name__ == "__main__":
    main()

