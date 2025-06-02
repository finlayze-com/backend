SELECT
    "Ticker" AS stock_ticker,
    "Sector" AS sector,
    "Market Cap" AS marketcap,
    "Value" AS value,
    ("Vol_Buy_I" - "Vol_Sell_I") AS net_haghighi,
    "Final" AS adjust_close,
    "Close(%)" AS price_change
FROM live_market_data
WHERE "Sector" IS NOT NULL
  AND "Ticker" IS NOT NULL
  AND "Market Cap" IS NOT NULL
  AND "Value" IS NOT NULL
  AND "Vol_Buy_I" IS NOT NULL
  AND "Vol_Sell_I" IS NOT NULL
  AND "Final" IS NOT NULL
  AND "Close(%)" IS NOT NULL
  AND "updated_at" = (
      SELECT MAX("updated_at") FROM live_market_data
  );
