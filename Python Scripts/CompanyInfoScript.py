import os
import asyncio
import aiohttp
import json
import boto3
import logging
from datetime import datetime
import pandas as pd
from aiohttp import ClientSession, ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed

# -----------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -----------------------------------------------------------
# Environment Variables
# -----------------------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not ALPHAVANTAGE_API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY environment variable.")

# -----------------------------------------------------------
# S3 Client
# -----------------------------------------------------------
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# -----------------------------------------------------------
# Fetch S&P 500 tickers
# -----------------------------------------------------------
def fetch_sp500_tickers():
    df = pd.read_csv("https://datahub.io/core/s-and-p-500-companies/r/constituents.csv")
    return df["Symbol"].str.upper().tolist()

# -----------------------------------------------------------
# Retry wrapper for fetching
# -----------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
async def fetch_overview(session: ClientSession, ticker: str):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=OVERVIEW&symbol={ticker}&apikey={ALPHAVANTAGE_API_KEY}"
    )

    async with session.get(url) as resp:
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status}")

        data = await resp.json()

    # Skip empty responses
    if not data or "Symbol" not in data:
        logging.warning(f"⚠️ No data for {ticker}")
        return None

    return data

# -----------------------------------------------------------
# Async fetch loop
# -----------------------------------------------------------
CALL_DELAY = 0.5  # premium-safe

async def fetch_all_overviews(tickers):
    timeout = ClientTimeout(total=120)
    connector = aiohttp.TCPConnector(limit=120)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for i, ticker in enumerate(tickers, start=1):
            logging.info(f"⏳ [{i}/{len(tickers)}] Fetching overview for {ticker}…")

            data = await fetch_overview(session, ticker)

            if data:
                yield ticker, data
                logging.info(f"✅ Completed {ticker}")
            else:
                logging.warning(f"❌ Skipped {ticker}")

            await asyncio.sleep(CALL_DELAY)

# -----------------------------------------------------------
# Save + Upload JSON
# -----------------------------------------------------------
def save_and_upload_json(ticker, data):
    filename = f"{ticker}.json"
    local_path = f"./{filename}"

    # Save JSON locally
    with open(local_path, "w") as f:
        json.dump(data, f)

    # Upload to S3
    s3_key = f"company_overview_json/{filename}"
    s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_key)

    logging.info(f"📤 Uploaded {filename} to s3://{S3_BUCKET_NAME}/{s3_key}")

    # Optional: delete local file afterward
    os.remove(local_path)

# -----------------------------------------------------------
# Main
# -----------------------------------------------------------
def main():
    logging.info("🚀 STARTING COMPANY OVERVIEW JSON INGEST")

    tickers = fetch_sp500_tickers()

    # Async driver
    async def run():
        async for ticker, data in fetch_all_overviews(tickers):
            save_and_upload_json(ticker, data)

    asyncio.run(run())

    logging.info("🎉 COMPLETE — All overview JSON files uploaded!")

# -----------------------------------------------------------
# Run
# -----------------------------------------------------------
if __name__ == "__main__":
    main()
