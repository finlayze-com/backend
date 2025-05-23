INSERT INTO daily_market_summary (date, stock_ticker, sector, value, market_cap, value_usd, market_cap_usd)
SELECT
    date_miladi::date AS date,
    stock_ticker,
    MAX(sector) AS sector,  -- فرض می‌گیریم در هر روز و نماد، فقط یک sector وجود داره
    SUM(value) AS value,
    SUM(adjust_close * share_number) AS market_cap,
    SUM(value_usd) AS value_usd,
    SUM((adjust_close * share_number) / dollar_rate) AS market_cap_usd
FROM
    daily_joined_data
GROUP BY
    date_miladi, stock_ticker
ON CONFLICT (date, stock_ticker) DO UPDATE SET
    sector = EXCLUDED.sector,
    value = EXCLUDED.value,
    market_cap = EXCLUDED.market_cap,
    value_usd = EXCLUDED.value_usd,
    market_cap_usd = EXCLUDED.market_cap_usd;
