CREATE DATABASE tradedb;

\c tradedb;

-- Create the raw schema
CREATE SCHEMA raw;

-- Create the trades table in the raw schema
CREATE TABLE raw.trades (
    account_id INT,
    symbol VARCHAR(255),
    side VARCHAR(255),
    qty INT,
    timestamp TIMESTAMP
);

-- Import data from the CSV file into the raw.trades table
COPY raw.trades FROM '/docker-entrypoint-initdb.d/trades.csv' DELIMITER ',' CSV HEADER;
