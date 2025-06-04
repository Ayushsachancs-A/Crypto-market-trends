import streamlit as st
import snowflake.connector
import pandas as pd

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='AYUSHSNOWFAKE',
    password='Ayush@snowflake7',
    account='FOIXSMX-GL06173',
    warehouse='COMPUTE_WH',
    database='CRYPTO_DATALAKE',
    schema='RAW_DATA'
)

# Query data
query = "SELECT * FROM CRYPTO_PRICES ORDER BY timestamp DESC LIMIT 100"
df = pd.read_sql(query, conn)

# Visualize
st.title("📊 Crypto Market Dashboard")
st.dataframe(df)
