-- backend/sql/orderbook_intrasector_timeseries.sql

SELECT
    ob."Sector" AS sector,
    ob."Symbol" AS "Symbol",
    sd.instrument_type AS instrument_type,   -- 👈 از symboldetail
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
    ON sd."Ticker" = ob."Symbol"   -- 👈 اگر ستون اسمش چیز دیگه‌ست، اینو تغییر بده
WHERE
    -- نرمال‌سازی sector با پارامتر :sector (مثل نسخه قبلی خودت)
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(trim(both FROM lower(ob."Sector")), 'ي','ی'),
        'ك','ک'),
      '‌',''),
    'ـ','')
    =
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(trim(both FROM lower(:sector)), 'ي','ی'),
        'ك','ک'),
      '‌',''),
    'ـ','')
  AND ob."Symbol" IS NOT NULL
GROUP BY sector, "Symbol", instrument_type, minute
ORDER BY minute;
