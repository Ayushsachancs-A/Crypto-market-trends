CREATE OR REPLACE DATABASE crypto_datalake;
USE DATABASE crypto_datalake;
CREATE OR REPLACE SCHEMA raw_data;


CREATE OR REPLACE TABLE raw_data.crypto_prices (
    coin STRING,
    price_usd FLOAT,
    timestamp TIMESTAMP
);

LIST @s3_stage;


COPY INTO raw_data.crypto_prices
FROM @s3_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

SELECT * FROM raw_data.crypto_prices
ORDER BY timestamp DESC
LIMIT 5;


SELECT coin, AVG(price_usd) AS avg_price
FROM raw_data.crypto_prices
GROUP BY coin;


SELECT
    coin,
    timestamp,
    price_usd,
    AVG(price_usd) OVER (PARTITION BY coin ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS moving_avg,
    LAG(price_usd, 24) OVER (PARTITION BY coin ORDER BY timestamp) AS price_24h_ago
FROM crypto_prices
ORDER BY timestamp DESC;