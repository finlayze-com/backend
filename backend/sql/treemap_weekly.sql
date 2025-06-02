WITH ranked AS (
    SELECT *,
           LAG(adjust_close) OVER (PARTITION BY stock_ticker ORDER BY week_start) AS prev_close,
           ROW_NUMBER() OVER (PARTITION BY stock_ticker ORDER BY week_start DESC) AS rn
    FROM weekly_joined_data
)
SELECT
    stock_ticker,
    sector,
    marketcap,
    value,
    (buy_i_value - sell_i_value) AS net_haghighi,
    adjust_close,
    prev_close,
    ROUND(100.0 * (adjust_close - prev_close) / NULLIF(prev_close, 0), 2) AS price_change
FROM ranked
WHERE rn = 1
  AND sector IS NOT NULL
  AND prev_close IS NOT NULL
  AND marketcap IS NOT NULL
  AND value IS NOT NULL
  AND buy_i_value IS NOT NULL
  AND sell_i_value IS NOT NULL
  AND adjust_close IS NOT NULL;
