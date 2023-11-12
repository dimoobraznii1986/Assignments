# Date Trades Analytics Solution

## Summary



## How to start

You can run the solution using `docker-compose up`. Before you will be run compose, we should upload data into the `02_data_platform/db/trades.csv` file in the format:

```
account_id,symbol,side,qty,timestamp
111111,GOOG,sell,10,2021-09-04 09:30:00
```
Then we can run the commands:

```
cd 02_data_platform/
docker-compose up --build
```

## What is Date Trades 
According to [documentation](https://docs.alpaca.markets/docs/user-protection)

A day trade is defined as a round-trip pair of trades within the same day (including extended hours). This is best described as an initial or opening transaction that is subsequently closed later in the same calendar day. For long positions, this would consist of a buy and then sell. For short positions, selling a security short and buying it back to cover the short position on the same day would also be considered a day trade.

Day trades are counted regardless of share quantity or frequency throughout the day.

## Solution 1


## Solution 2