import os
import time
import json
import requests
import pandas as pd
import boto3
from datetime import datetime

# ---- Load credentials from environment variables ----
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region,
)

local_dir = "./stock_data"
os.makedirs(local_dir, exist_ok=True)

CHECKPOINT_FILE = os.path.join(local_dir, "processed_tickers.json")

# ---- Load checkpoint progress if exists ----
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        processed = set(json.load(f))
else:
    processed = set()

stock_list = [
    "RIVN","NVDA","PLUG","PLTR","PFE","TSLA","F","AMD","PINS","DNN","BITF",
    "CIFR","BBAI","INTC","RXRX","SMCI","OPEN","AAL","SOFI","ONDS","TEVA","SNAP",
    "BAC","ACHR","KVUE","FUBO","QS","IREN","VALE","MARA","NVO","RIG","WULF","T","BTG",
    "U","HIMS","RGTI","GRAB","NIO","BMNR","NOK","WBD","CMCSA","PCG","NCLH","RIOT","AG","QBTS",
    "NU","AMZN","NVTS","ABEV","JOBY","BBD","CMG","SOUN","AAPL","PATH","MU","AMCR","PBR","ETNB",
    "NGD","GOOGL","ITUB","TREX","UPST","HOOD","RKT","CPNG","ZETA","APLD","NKE","META",
    "AUR","HBAN","CLSK","HPE","TOST","QUBT","AGNC","CCCS","IAG","STLA","UUUU","RF",
    "CCL","CDE","EXK","CRWV","CLF","GT","VZ","GGB","UBER","KEY","CRBG","IONQ","MSFT","LUMN",
    "JHX","LMND","LYFT"
]

all_records = []

def fetch_company_overview(ticker):
    url = (
        f"https://www.alphavantage.co/query?function=OVERVIEW"
        f"&symbol={ticker}&apikey={api_key}"
    )

    attempt = 1
    while attempt <= 6:  # will retry up to 6 times
        try:
            response = requests.get(url, timeout=30)

            if response.status_code != 200:
                print(f"⚠ HTTP {response.status_code} — retrying in {attempt * 5}s...")
                time.sleep(attempt * 5)
                attempt += 1
                continue

            data = response.json()

            # Check for API throttling note → wait 60s and retry
            if "Note" in data or data == {}:
                print(f"⏳ Rate limited — waiting 65 seconds...")
                time.sleep(65)
                attempt += 1
                continue

            # Valid data
            data["ticker"] = ticker
            return data

        except requests.exceptions.Timeout:
            print(f"⏱ Timeout — retrying in {attempt * 5}s...")
            time.sleep(attempt * 5)
            attempt += 1

        except Exception as e:
            print(f"❌ Unexpected error: {e} — retrying...")
            time.sleep(attempt * 5)
            attempt += 1

    return None


for ticker in stock_list:
    if ticker in processed:
        print(f"⏭ Skipping {ticker}, already processed.")
        continue

    print(f"\n📥 Fetching {ticker}...")
    result = fetch_company_overview(ticker)

    if result:
        all_records.append(result)
        print(f"✅ Retrieved {ticker}")
    else:
        print(f"❌ No data for {ticker}, moving on.")

    # Save progress incrementally
    processed.add(ticker)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(processed), f)


if all_records:
    combined_df = pd.DataFrame(all_records)
    output_file = os.path.join(local_dir, "combined_company_overview.csv")
    combined_df.to_csv(output_file, index=False)

    print(f"\n✅ Saved locally: {output_file}")

    s3_client.upload_file(output_file, s3_bucket_name, "combined_company_overview.csv")
    print(f"🚀 Uploaded to S3: s3://{s3_bucket_name}/combined_company_overview.csv\n")
else:
    print("\n⚠ No data collected.")
