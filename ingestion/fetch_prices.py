import requests
import pandas as pd
from datetime import datetime, UTC
import boto3
from io import StringIO
import os

def fetch_prices():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dogecoin&vs_currencies=usd"
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data).T
    df.columns = ['price_usd']
    df['timestamp'] = datetime.now(UTC)
    df.index.name = 'coin'
    return df.reset_index()

def clean_data(df):
    df = df.dropna()
    df = df[df['price_usd'] > 0]
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

def upload_to_s3(df, bucket, filename):
    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload using boto3
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=filename, Body=csv_buffer.getvalue())

if __name__ == "__main__":
    df = fetch_prices()
    df = clean_data(df)
    filename = f"cleaned/crypto_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.csv"
    upload_to_s3(df, 'crypto-transform-bucket', filename)
    print("✅ Uploaded to S3:", filename)
