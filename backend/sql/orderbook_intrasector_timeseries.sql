SELECT
    ob."Sector"       AS "Sector",
    ob."Symbol"       AS "Symbol",
    sd.instrument_type AS instrument_type,
    date_trunc('minute', ob."Timestamp") AS minute,
    COALESCE(SUM(
        COALESCE(ob."BuyPrice1", 0) * COALESCE(ob."BuyVolume1", 0) +
        COALESCE(ob."BuyPrice2", 0) * COALESCE(ob."BuyVolume2", 0) +
        COALESCE(ob."BuyPrice3", 0) * COALESCE(ob."BuyVolume3", 0) +
        COALESCE(ob."BuyPrice4", 0) * COALESCE(ob."BuyVolume4", 0) +
        COALESCE(ob."BuyPrice5", 0) * COALESCE(ob."BuyVolume5", 0)
    ), 0) AS total_buy,
    COALESCE(SUM(
        COALESCE(ob."SellPrice1", 0) * COALESCE(ob."SellVolume1", 0) +
        COALESCE(ob."SellPrice2", 0) * COALESCE(ob."SellVolume2", 0) +
        COALESCE(ob."SellPrice3", 0) * COALESCE(ob."SellVolume3", 0) +
        COALESCE(ob."SellPrice4", 0) * COALESCE(ob."SellVolume4", 0) +
        COALESCE(ob."SellPrice5", 0) * COALESCE(ob."SellVolume5", 0)
    ), 0) AS total_sell
FROM orderbook_snapshot ob
JOIN symboldetail sd
    ON sd."Ticker" = ob."Symbol"   -- اگه اسماش فرق داره این خط رو با اسمای خودت اصلاح کن
WHERE
    ob."Symbol" IS NOT NULL
    AND ob."Sector" IS NOT NULL
    AND REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(trim(both FROM lower(ob."Sector")), 'ي','ی'),
            'ك','ک'),
          '‌',''),
        'ـ',''
    )
    =
    REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(trim(both FROM lower(:sector)), 'ي','ی'),
            'ك','ک'),
          '‌',''),
        'ـ',''
    )
GROUP BY ob."Sector", ob."Symbol", sd.instrument_type, minute
ORDER BY minute;
