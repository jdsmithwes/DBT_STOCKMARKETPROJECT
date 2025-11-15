import os
import boto3
import pandas as pd
import yfinance as yf
import concurrent.futures
import time
import requests

# ---------------------------------------------------------------------
# AWS Credentials from Environment Variables
# ---------------------------------------------------------------------
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")

# ---------------------------------------------------------------------
# Create S3 Client
# ---------------------------------------------------------------------
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# ---------------------------------------------------------------------
# 1️⃣ Fetch All US Stock Tickers (Reliable Source)
# ---------------------------------------------------------------------
TICKER_SOURCE_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.csv"

def fetch_us_tickers():
    print("🔍 Fetching US equity tickers from Github (reliable source)...")
    df = pd.read_csv(TICKER_SOURCE_URL)
    tickers = df["Symbol"].dropna().unique().tolist()
    print(f"✅ Found {len(tickers)} tickers")
    return tickers

# ---------------------------------------------------------------------
# 2️⃣ Fetch Daily Price Data Using yfinance + Retry Logic
# ---------------------------------------------------------------------
def fetch_data_for_ticker(ticker, retries=3):
    for attempt in range(1, retries + 1):
        try:
            data = yf.download(ticker, period="1y", interval="1d", progress=False)
            if not data.empty:
                data["ticker"] = ticker
                return data
            return None
        except Exception as e:
            print(f"⚠️ Error fetching {ticker} (attempt {attempt}): {e}")
            time.sleep(1 * attempt)  # linear backoff
    return None

# ---------------------------------------------------------------------
# 3️⃣ Multi-threading for Speed
# ---------------------------------------------------------------------
def fetch_all_data(tickers):
    print("🚀 Fetching stock data using multi-threading...")

    dataframes = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_data_for_ticker, t): t for t in tickers}

        for future in concurrent.futures.as_completed(futures):
            ticker = futures[future]
            df = future.result()
            if df is not None:
                print(f"✅ Retrieved {ticker}")
                dataframes.append(df)
            else:
                print(f"⚠️ No data for {ticker}")

    return dataframes

# ---------------------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------------------
print("🔍 Fetching all US equity tickers...")
tickers = fetch_us_tickers()

print("📊 Gathering historical daily data...")
all_dfs = fetch_all_data(tickers)

if not all_dfs:
    print("❌ No data fetched.")
    exit()

# Combine into master DataFrame
combined = pd.concat(all_dfs)
combined.reset_index(inplace=True)

# Save to temporary GitHub Actions directory
output_file = "combined_stock_data.csv"
combined.to_csv(output_file, index=False)

print(f"📁 Saved file: {output_file}")

# Upload to S3
s3_client.upload_file(output_file, s3_bucket_name, "combined_stock_data.csv")
print(f"🚀 Uploaded to s3://{s3_bucket_name}/combined_stock_data.csv")
