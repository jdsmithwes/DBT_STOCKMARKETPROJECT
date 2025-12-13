import os
import asyncio
import aiohttp
import pandas as pd
import boto3
import json
import logging
from datetime import datetime
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
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# -----------------------------------------------------------
# Load S&P500 Tickers
# -----------------------------------------------------------
def fetch_sp500_tickers():
    df = pd.read_csv("https://datahub.io/core/s-and-p-500-companies/r/constituents.csv")
    return df["Symbol"].str.upper().tolist()

# -----------------------------------------------------------
# Fetch overview JSON (with retries)
# -----------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(0.5))
async def fetch_overview(session, ticker):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=OVERVIEW&symbol={ticker}&apikey={ALPHAVANTAGE_API_KEY}"
    )

    async with session.get(url) as resp:
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status}")
        data = await resp.json()

    if not data or "Symbol" not in data:
        logging.warning(f"⚠️ {ticker}: No overview data returned.")
        return None

    data["ticker"] = ticker
    data["ingest_timestamp"] = datetime.utcnow().isoformat()

    return data

# -----------------------------------------------------------
# Direct-to-S3 JSON uploader
# -----------------------------------------------------------
def upload_json_to_s3(records):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    s3_key = f"company_overview_json/overview_{timestamp}.json"

    json_body = "\n".join(json.dumps(rec) for rec in records)

    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=json_body.encode("utf-8"),
        ContentType="application/json"
    )

    logging.info(f"📤 Uploaded JSON → s3://{S3_BUCKET_NAME}/{s3_key}")

# -----------------------------------------------------------
# Async runner
# -----------------------------------------------------------
async def fetch_all_overviews(tickers):
    timeout = ClientTimeout(total=120)
    connector = aiohttp.TCPConnector(limit=120)

    all_records = []

    async with ClientSession(timeout=timeout, connector=connector) as session:
        for i, ticker in enumerate(tickers, start=1):
            logging.info(f"⏳ [{i}/{len(tickers)}] Fetching OVERVIEW for {ticker}…")
            record = await fetch_overview(session, ticker)

            if record:
                all_records.append(record)
                logging.info(f"✅ {ticker} done")
            else:
                logging.warning(f"❌ No data for {ticker}")

            await asyncio.sleep(0.5)

    return all_records

# -----------------------------------------------------------
# Main
# -----------------------------------------------------------
def main():
    logging.info("🚀 STARTING ZERO-LOCAL JSON COMPANY OVERVIEW INGEST")

    tickers = fetch_sp500_tickers()
    records = asyncio.run(fetch_all_overviews(tickers))

    if not records:
        logging.warning("⚠️ No company overview data returned.")
        return

    upload_json_to_s3(records)

    logging.info("🎉 COMPLETE — JSON uploaded directly to S3!")

if __name__ == "__main__":
    main()
