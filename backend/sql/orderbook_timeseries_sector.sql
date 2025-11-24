-- backend/sql/orderbook_timeseries_sector.sql

WITH latest_day AS (
    SELECT MAX("Timestamp"::date) AS d
    FROM orderbook_snapshot
)
SELECT
    s."Sector" AS sector,
    date_trunc('minute', s."Timestamp") AS minute,
    SUM(
        COALESCE(s."BuyPrice1", 0) * COALESCE(s."BuyVolume1", 0) +
        COALESCE(s."BuyPrice2", 0) * COALESCE(s."BuyVolume2", 0) +
        COALESCE(s."BuyPrice3", 0) * COALESCE(s."BuyVolume3", 0) +
        COALESCE(s."BuyPrice4", 0) * COALESCE(s."BuyVolume4", 0) +
        COALESCE(s."BuyPrice5", 0) * COALESCE(s."BuyVolume5", 0)
    ) AS total_buy,
    SUM(
        COALESCE(s."SellPrice1", 0) * COALESCE(s."SellVolume1", 0) +
        COALESCE(s."SellPrice2", 0) * COALESCE(s."SellVolume2", 0) +
        COALESCE(s."SellPrice3", 0) * COALESCE(s."SellVolume3", 0) +
        COALESCE(s."SellPrice4", 0) * COALESCE(s."SellVolume4", 0) +
        COALESCE(s."SellPrice5", 0) * COALESCE(s."SellVolume5", 0)
    ) AS total_sell
FROM orderbook_snapshot s
JOIN latest_day ld
    ON s."Timestamp"::date = ld.d
WHERE
    s."Sector" IS NOT NULL
GROUP BY
    sector,
    minute
ORDER BY
    minute;
