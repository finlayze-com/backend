SELECT
    "Sector",
    date_trunc('minute', "Timestamp") AS minute,
    SUM(COALESCE("BuyPrice1", 0) * COALESCE("BuyVolume1", 0) +
        COALESCE("BuyPrice2", 0) * COALESCE("BuyVolume2", 0) +
        COALESCE("BuyPrice3", 0) * COALESCE("BuyVolume3", 0) +
        COALESCE("BuyPrice4", 0) * COALESCE("BuyVolume4", 0) +
        COALESCE("BuyPrice5", 0) * COALESCE("BuyVolume5", 0)) AS total_buy,
    SUM(COALESCE("SellPrice1", 0) * COALESCE("SellVolume1", 0) +
        COALESCE("SellPrice2", 0) * COALESCE("SellVolume2", 0) +
        COALESCE("SellPrice3", 0) * COALESCE("SellVolume3", 0) +
        COALESCE("SellPrice4", 0) * COALESCE("SellVolume4", 0) +
        COALESCE("SellPrice5", 0) * COALESCE("SellVolume5", 0)) AS total_sell
FROM orderbook_snapshot
WHERE "Timestamp"::date = CURRENT_DATE
GROUP BY "Sector", minute
ORDER BY minute;