import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px


#Connect to Snowflake
conn = snowflake.connector.connect(
    user='YOUR_USER',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',
    warehouse='YOUR_WH',
    database='YOUR_DB',
    schema='YOUR_SCHEMA'
)


# Query Data from Snowflake
desc_query = "DESC TABLE CRYPTO_PRICES"
cursor = conn.cursor()
cursor.execute(desc_query)
print("Table Structure:")
for col in cursor.fetchall():
    print(col)

# Now get the data
query = "SELECT * FROM CRYPTO_PRICES ORDER BY TIMESTAMP DESC LIMIT 500"
df = pd.read_sql(query, conn)
print("\nDataFrame Columns:", df.columns.tolist())
print("\nFirst few rows of data:")
print(df.head())

conn.close()


# Data Cleaning & Enrichment
df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']) 
df = df.sort_values(by=["COIN", "TIMESTAMP"])

# Calculate 24h ago price and % change
df['price_24h_ago'] = df.groupby('COIN')['PRICE_USD'].shift(1)
df['24h_change'] = ((df['PRICE_USD'] - df['price_24h_ago']) / df['price_24h_ago']) * 100

# Moving average
df['moving_avg'] = df.groupby('COIN')['PRICE_USD'].rolling(window=6).mean().reset_index(level=0, drop=True)

# Rename columns 
df = df.rename(columns={
    'TIMESTAMP': 'timestamp',
    'COIN': 'coin',
    'PRICE_USD': 'price_usd'
})


# Streamlit UI
st.title("📈 Crypto Market Trend Analyzer")

# Show most recent prices
st.subheader("🪙 Latest Prices")
latest = df.groupby("coin").tail(1)[['coin', 'price_usd', '24h_change']]
st.dataframe(latest)

# Coin selector
coin = st.selectbox("Choose a cryptocurrency", df['coin'].unique())
coin_df = df[df['coin'] == coin]

# Line chart: Price trend
st.subheader(f"{coin} Price Trend")
st.plotly_chart(px.line(coin_df, x='timestamp', y='price_usd', title=f"{coin} Price"))

# Line chart: Moving average
st.subheader(f"{coin} Moving Average")
st.plotly_chart(px.line(coin_df, x='timestamp', y='moving_avg', title=f"{coin} Moving Average (6 samples)"))

# Line chart: 24h Change %
st.subheader(f"{coin} 24h Price Change (%)")
st.line_chart(coin_df.set_index('timestamp')['24h_change'])
