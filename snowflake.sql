-- Create database and schema
CREATE OR REPLACE DATABASE crypto_datalake;
USE DATABASE crypto_datalake;
CREATE OR REPLACE SCHEMA raw_data;

-- Create target table for crypto prices
CREATE OR REPLACE TABLE raw_data.crypto_prices (
    coin STRING,
    price_usd FLOAT,
    timestamp TIMESTAMP
);

-- Create external stage to point to your S3 bucket
CREATE OR REPLACE STAGE s3_stage
URL='s3://crypto-transform-bucket/raw/'
STORAGE_INTEGRATION = (skip this for now);
--access credentials here

-- View files in the S3 stage
LIST @s3_stage;

-- Copy CSV from S3 to Snowflake table
COPY INTO raw_data.crypto_prices
FROM @s3_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Show the latest 5 entries
SELECT * FROM raw_data.crypto_prices
ORDER BY timestamp DESC
LIMIT 5;

-- Average price by coin
SELECT coin, AVG(price_usd) AS avg_price
FROM raw_data.crypto_prices
GROUP BY coin;

--Trend Matrics
SELECT
    coin,
    timestamp,
    price_usd,
    AVG(price_usd) OVER (PARTITION BY coin ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS moving_avg,
    LAG(price_usd, 24) OVER (PARTITION BY coin ORDER BY timestamp) AS price_24h_ago
FROM crypto_prices
ORDER BY timestamp DESC;
