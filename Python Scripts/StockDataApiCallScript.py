import os
import asyncio
import aiohttp
import pandas as pd
import boto3
import logging
from datetime import datetime
from aiohttp import ClientSession, ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed

# -----------------------------------------------------------
# CONFIGURABLE START DATE (CONTROLS WHAT GETS LOADED)
# -----------------------------------------------------------
START_DATE = pd.to_datetime("2026-11-14")  # <-- YOU CONTROL YOUR PIPELINE FROM HERE

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
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")  # <-- MUST BE SET IN EC2 ENV VARS

if not ALPHAVANTAGE_API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY environment variable.")

if not SNS_TOPIC_ARN:
    raise ValueError("Missing SNS_TOPIC_ARN environment variable.")


# ------------------------------------
# AWS Clients
# ------------------------------------
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

sns_client = boto3.client("sns", region_name=AWS_REGION)


# ------------------------------------
# SNS Notification Functions
# ------------------------------------
def notify_success(message="EC2 Stock API job completed successfully."):
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="EC2 Job Success",
            Message=message,
        )
        logging.info("📨 SNS success notification sent.")
    except Exception as e:
        logging.error(f"⚠️ Failed to send SNS success notification: {e}")


def notify_failure(error_message):
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="EC2 Job FAILURE",
            Message=f"Stock API job FAILED.\n\nError:\n{error_message}",
        )
        logging.info("📨 SNS failure notification sent.")
    except Exception as e:
        logging.error(f"⚠️ Failed to send SNS failure notification: {e}")


# ------------------------------------
# Fetch S&P 500 Tickers
# ------------------------------------
def fetch_sp500_tickers():
    df = pd.read_csv("https://datahub.io/core/s-and-p-500-companies/r/constituents.csv")
    return df["Symbol"].str.upper().tolist()


# ------------------------------------
# Normalize AlphaVantage JSON → DataFrame
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
# Apply START_DATE Filter
# ------------------------------------
def filter_to_start_date(df, ticker):
    filtered = df[df["date"] >= START_DATE]

    logging.info(
        f"⏳ {ticker}: Filtered {df.shape[0]} → {filtered.shape[0]} rows after {START_DATE.date()}"
    )

    return filtered


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
        logging.warning(f"⚠️ {ticker}: No time series returned.")
        return None

    df = normalize_alpha_vantage(ticker, ts)

    # Incremental cutoff filtering
    df = filter_to_start_date(df, ticker)

    return df if not df.empty else None


# ------------------------------------
# Rate Limiting
# ------------------------------------
API_CALLS_PER_MINUTE = 110
CALL_DELAY = 60 / API_CALLS_PER_MINUTE


# ------------------------------------
# Async Ticker Fetch Loop
# ------------------------------------
async def fetch_all_full_history(tickers):
    timeout = ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=50)
    results = []

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for i, ticker in enumerate(tickers, start=1):

            logging.info(f"⏳ [{i}/{len(tickers)}] Fetching {ticker}…")

            df = await fetch_full_history(session, ticker)
            if df is not None:
                results.append(df)
                logging.info(f"✅ Completed {ticker}")
            else:
                logging.warning(f"❌ No rows retained for {ticker}")

            await asyncio.sleep(CALL_DELAY)

    return results


# ------------------------------------
# S3 Upload
# ------------------------------------
def upload_partitioned(df, ticker):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    s3_prefix = f"stock_data/ticker={ticker}/"
    filename = f"stock_data_{timestamp}.csv"
    local_path = f"./{filename}"

    df.to_csv(local_path, index=False)
    s3_key = f"{s3_prefix}{filename}"
    s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_key)

    logging.info(f"📤 Uploaded → s3://{S3_BUCKET_NAME}/{s3_key}")


# ------------------------------------
# Main Workflow with SNS Success/Failure
# ------------------------------------
def main():
    try:
        logging.info("🚀 STARTING INCREMENTAL HISTORICAL INGEST (DATE-CUTOFF MODE)")

        tickers = fetch_sp500_tickers()
        all_data = asyncio.run(fetch_all_full_history(tickers))

        if not all_data:
            raise Exception("No data returned from API calls.")

        for df in all_data:
            ticker = df["ticker"].iloc[0]
            upload_partitioned(df, ticker)

        logging.info("🎉 COMPLETE — Incremental batches uploaded!")
        notify_success("EC2 stock ingestion job completed successfully.")

    except Exception as e:
        logging.error(f"❌ JOB FAILED: {e}")
        notify_failure(str(e))
        raise  # re-raise for cron/log visibility


# ------------------------------------
# Run
# ------------------------------------
if __name__ == "__main__":
    main()

    #trigger for git add

