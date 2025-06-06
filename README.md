# 🪙 Crypto Market Data Lake

A real-time crypto market analytics pipeline that ingests, stores, transforms, and visualizes cryptocurrency data using modern data engineering tools.

## 🚀 Project Overview

This project builds a complete data pipeline for tracking and analyzing real-time cryptocurrency prices. It fetches price data from the CoinGecko API, stores raw CSV files in **AWS S3**, loads the data into **Snowflake**, and builds interactive dashboards using **Streamlit** and **Plotly** for real-time trend analysis.

## 🛠️ Tech Stack

- **Data Source:** CoinGecko API
- **Programming:** Python (requests, pandas, boto3)
- **Storage:** AWS S3 (Raw data lake storage)
- **Data Warehouse:** Snowflake (External stage + SQL transformation)
- **Dashboard:** Streamlit + Plotly

## 🔄 How It Works

1. **Ingestion**  
   - `fetch_price.py` connects to the CoinGecko API, extracts the latest prices for Bitcoin, Ethereum, and Dogecoin.
   - The data is cleaned, stored in timestamped CSV files and uploaded to a specified S3 bucket.

2. **Data Lake & Warehouse**  
   - Data is stored in **AWS S3** (your data lake).
   - **Snowflake** is configured with an external stage to load data from S3 into structured tables.

3. **Transformation & Trend Analysis**  
   - SQL queries calculate trends, price changes, and averages inside Snowflake.(`snowflake.sql`)

4. **Visualization**  
   - **Streamlit** dashboard (`streamlit_snowflake.py`) fetches live data from Snowflake and visualizes:
     - Latest prices
     - Price trends over time
     - 24-hour % change

## 📊 Features

- Real-time crypto data ingestion
- Cloud-based data storage and processing
- Dynamic dashboard with historical trend analysis
- Extensible architecture for more coins or metrics

## 👨‍💻 Author

**Ayush Sachan** – aspiring data engineer  
This project demonstrates core concepts of modern data engineering including ETL, cloud storage, data lakes, SQL-based analytics, and dashboarding.

---
