  WITH buy_sell_pairs AS (
  SELECT 
    account_id,
    symbol,
    side,
    qty,
    timestamp,
    DATE(timestamp) AS date,
    LAG(side) OVER w AS prev_side
  FROM 
   {{ source('raw','trades')}}
  WINDOW w AS (
    PARTITION BY account_id, symbol, DATE(timestamp)
    ORDER BY timestamp
  )
)
SELECT 
  account_id,
  COUNT(*) AS pdt_count
FROM 
  buy_sell_pairs
WHERE 
  side = 'sell' AND prev_side = 'buy'
GROUP BY 
  account_id
ORDER BY 
  pdt_count desc