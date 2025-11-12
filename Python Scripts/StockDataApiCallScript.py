import ssl
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
import os
import boto3

# ---- Set your AWS credentials directly here ----
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")

# Create S3 client with the above credentials
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# ---- Output Directory ----
local_dir = "./stock_data"
os.makedirs(local_dir, exist_ok=True)

# ---- List of Tickers ----
dow_jones_stocks = [
    "RIVN","NVDA","PLUG","PLTR","PFE","TSLA","F","AMD","PINS","DNN","BITF",
    "CIFR","BBAI","INTC","RXRX","SMCI","OPEN","AAL","SOFI","ONDS","TEVA","SNAP",
    "BAC","ACHR","KVUE","FUBO","QS","IREN","VALE","MARA","NVO","RIG","WULF","T","BTG",
    "U","HIMS","RGTI","GRAB","NIO","BMNR","NOK","WBD","CMCSA","PCG","NCLH","RIOT","AG","QBTS",
    "NU","AMZN","NVTS","ABEV","JOBY","BBD","CMG","SOUN","AAPL","PATH","MU","AMCR","PBR","ETNB",
    "NGD","GOOGL","ITUB","TREX","UPST","HOOD","RKT","CPNG","ZETA","APLD","NKE","META",
    "AUR","HBAN","CLSK","HPE","TOST","QUBT","AGNC","CCCS","IAG","STLA","UUUU","RF",
    "CCL","CDE","EXK","CRWV","CLF","GT","VZ","GGB","UBER","KEY","CRBG","IONQ", "MSFT","LUMN",
    "JHX","LMND","LYFT"]

# ---- Alpha Vantage API Key ----
api_key = "NY03HQLBAOIVQ6AF"

all_dataframes = []

for ticker in dow_jones_stocks:
    print(f"Fetching daily data for {ticker}...")

    url = (
        f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY"
        f"&symbol={ticker}&apikey={api_key}"
    )
    response = requests.get(url)

    if response.status_code == 200:
        json_data = response.json()
        time_series = json_data.get("Time Series (Daily)")

        if time_series:
            df = pd.DataFrame.from_dict(time_series, orient="index")
            df["ticker"] = ticker
            all_dataframes.append(df)
            print(f"✅ Retrieved {ticker}")
        else:
            print(f"⚠ No time series data for {ticker}")
    else:
        print(f"❌ Request failed for {ticker} (HTTP {response.status_code})")

# ---- Combine all stocks and upload ----
if all_dataframes:
    combined_df = pd.concat(all_dataframes)
    combined_df.index.name = "date"
    combined_df.reset_index(inplace=True)

    combined_file_path = os.path.join(local_dir, "combined_stock_data.csv")
    combined_df.to_csv(combined_file_path, index=False)
    print(f"\n✅ Saved locally: {combined_file_path}")

    # ---- Upload unified CSV to S3 ----
    s3_client.upload_file(combined_file_path, s3_bucket_name, "combined_stock_data.csv")
    print(f"🚀 Uploaded to S3: s3://{s3_bucket_name}/combined_stock_data.csv")

else:
    print("\n⚠ No data retrieved — nothing uploaded.")

