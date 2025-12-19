import os
import re
import asyncio
import aiohttp
import pandas as pd
import boto3
import logging
from datetime import datetime, timedelta, date
from aiohttp import ClientSession, ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed

# -----------------------------------------------------------
# LOGGING
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -----------------------------------------------------------
# EXISTING ENVIRONMENT VARIABLES ONLY
# -----------------------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not S3_BUCKET_NAME:
    raise ValueError("Missing S3_BUCKET_NAME environment variable")
if not ALPHAVANTAGE_API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY environment variable")

# -----------------------------------------------------------
# INTERNAL CONSTANTS (NOT ENV VARS)
# -----------------------------------------------------------
S3_PREFIX = "stock_prices/"
DATE_REGEX = re.compile(r"(\d{4}-\d{2}-\d{2})")
FALLBACK_START_DATE = date(2020, 1, 1)
BOOTSTRAP_TICKER = "AAPL"  # used only if S3 is empty

# -----------------------------------------------------------
# AWS CLIENT
# -----------------------------------------------------------
s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# -----------------------------------------------------------
# DISCOVER LAST LOADED DATE FROM S3
# -----------------------------------------------------------
def get_last_loaded_date_from_s3() -> date:
    paginator = s3_client.get_paginator("list_objects_v2")
    latest_date = None

    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME,
        Prefix=S3_PREFIX,
    ):
        for obj in page.get("Contents", []):
            match = DATE_REGEX.search(obj["Key"])
            if match:
                parsed_date = datetime.strptime(
                    match.group(1), "%Y-%m-%d"
                ).date()
                if not latest_date or parsed_date > latest_date:
                    latest_date = parsed_date

    if latest_date:
        logging.info(f"📦 Last loaded S3 data date: {latest_date}")
        return latest_date

    logging.warning("⚠️ No dated objects found in S3")
    return FALLBACK_START_DATE


# -----------------------------------------------------------
# DISCOVER TICKERS DYNAMICALLY FROM S3 DATA
# -----------------------------------------------------------
def discover_tickers_from_s3() -> list[str]:
    """
    Reads a small sample of existing S3 files to infer tickers dynamically.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    tickers = set()

    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME,
        Prefix=S3_PREFIX,
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                response = s3_client.get_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=obj["Key"],
                )
                df = pd.read_csv(response["Body"], nrows=500)
                if "ticker" in df.columns:
                    tickers.update(df["ticker"].dropna().unique())

            if tickers:
                break

        if tickers:
            break

    if tickers:
        ticker_list = sorted(tickers)
        logging.info(f"📈 Discovered {len(ticker_list)} tickers dynamically")
        return ticker_list

    logging.warning(
        "⚠️ No existing ticker data found — using bootstrap ticker"
    )
    return [BOOTSTRAP_TICKER]


# -----------------------------------------------------------
# DETERMINE API DATE WINDOW
# -----------------------------------------------------------
def determine_api_date_window():
    last_loaded_date = get_last_loaded_date_from_s3()
    start_date = last_loaded_date + timedelta(days=1)
    end_date = date.today()

    if start_date > end_date:
        logging.info("✅ No new data to load — exiting")
        return None, None

    logging.info(f"📅 API window: {start_date} → {end_date}")
    return start_date, end_date


# -----------------------------------------------------------
# API CALL
# -----------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def fetch_stock_data(session, ticker, start_date, end_date):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={ticker}"
        f"&apikey={ALPHAVANTAGE_API_KEY}"
        f"&outputsize=full"
    )

    async with session.get(url) as response:
        response.raise_for_status()
        data = await response.json()

        if "Time Series (Daily)" not in data:
            raise ValueError(f"Invalid API response for {ticker}")

        df = (
            pd.DataFrame.from_dict(
                data["Time Series (Daily)"], orient="index"
            )
            .reset_index()
            .rename(columns={"index": "date"})
        )

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
        df["ticker"] = ticker

        return df


# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
async def main():
    start_date, end_date = determine_api_date_window()
    if not start_date:
        return

    tickers = discover_tickers_from_s3()

    timeout = ClientTimeout(total=60)
    async with ClientSession(timeout=timeout) as session:
        tasks = [
            fetch_stock_data(session, ticker, start_date, end_date)
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks)

    final_df = pd.concat(results, ignore_index=True)

    if final_df.empty:
        logging.info("⚠️ API returned no new rows — exiting")
        return

    file_date = end_date.strftime("%Y-%m-%d")
    local_file = "/tmp/stock_prices.csv"
    s3_key = f"{S3_PREFIX}{file_date}_stock_prices.csv"

    final_df.to_csv(local_file, index=False)

    s3_client.upload_file(
        local_file,
        S3_BUCKET_NAME,
        s3_key,
    )

    logging.info(
        f"✅ Uploaded {len(final_df)} rows to "
        f"s3://{S3_BUCKET_NAME}/{s3_key}"
    )


# -----------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())