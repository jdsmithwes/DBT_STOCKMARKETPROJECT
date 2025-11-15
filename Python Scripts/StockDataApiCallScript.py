import os
import asyncio
import aiohttp
import pandas as pd
import boto3
import logging
from datetime import datetime
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
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
alpha_vantage_api_key = os.getenv("ALPHAVANTAGE_API_KEY")

if not alpha_vantage_api_key:
    raise ValueError("Missing Alpha Vantage key. Set ALPHAVANTAGE_API_KEY.")

# ------------------------------------
# Create S3 Client
# ------------------------------------
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region,
)

# Output folder (local)
local_dir = "./stock_data"
os.makedirs(local_dir, exist_ok=True)

# ------------------------------------
# Fetch S&P 500 Tickers (DataHub – most reliable)
# ------------------------------------
def fetch_sp500_tickers():
    logging.info("🔍 Fetching S&P 500 tickers from DataHub…")
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"

    try:
        df = pd.read_csv(url)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch S&P500 list: {e}")

    if "Symbol" not in df.columns:
        raise RuntimeError("S&P500 list missing expected 'Symbol' column.")

    tickers = df["Symbol"].str.upper().tolist()
    logging.info(f"✅ Loaded {len(tickers)} S&P 500 tickers.")
    return tickers

# ------------------------------------
# Async Alpha Vantage Fetch
# ------------------------------------
async def fetch_daily_close(session: ClientSession, ticker: str):
    """Async fetch TIME_SERIES_DAILY from Alpha Vantage."""
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={alpha_vantage_api_key}"
    )

    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logging.warning(f"⚠️ {ticker}: HTTP {resp.status}")
                return None

            data = await resp.json()

    except Exception as e:
        logging.warning(f"⚠️ {ticker}: API request failed: {e}")
        return None

    # Parse the JSON response
    ts = data.get("Time Series (Daily)")
    if ts is None:
        # API hit limit or no data
        return None

    try:
        df = pd.DataFrame.from_dict(ts, orient="index").reset_index()
        df.columns = ["date"] + list(df.columns[1:])
        df["ticker"] = ticker
        return df
    except Exception as e:
        logging.warning(f"⚠️ {ticker}: Failed to parse daily time series: {e}")
        return None

# ------------------------------------
# Async Driver (parallel fetching)
# ------------------------------------
async def fetch_all_tickers(tickers):
    timeout = ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=100)  # massive speed boost

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = []
        for ticker in tickers:
            tasks.append(fetch_daily_close(session, ticker))

        logging.info(f"🚀 Launching {len(tasks)} async API calls…")

        results = await asyncio.gather(*tasks)

    return [df for df in results if df is not None]

# ------------------------------------
# Main
# ------------------------------------
def main():
    logging.info("🔍 Starting async S&P500 Alpha Vantage daily close fetch...")

    # 1️⃣ Fetch S&P500 Tickers
    tickers = fetch_sp500_tickers()

    # 2️⃣ Run async fetcher
    all_frames = asyncio.run(fetch_all_tickers(tickers))

    if not all_frames:
        logging.error("❌ No data returned for any tickers.")
        return

    # 3️⃣ Combine + save to file
    combined = pd.concat(all_frames)
    output_file = os.path.join(local_dir, "combinedstockdata.csv")
    combined.to_csv(output_file, index=False)

    logging.info(f"💾 Saved → {output_file}")

    # 4️⃣ Upload to S3
    s3_client.upload_file(output_file, s3_bucket_name, "combinedstockdata.csv")

    logging.info(f"🚀 Uploaded to S3 → s3://{s3_bucket_name}/combinedstockdata.csv")
    logging.info("🎉 Script complete!")

# ------------------------------------
# Run
# ------------------------------------
if __name__ == "__main__":
    main()

