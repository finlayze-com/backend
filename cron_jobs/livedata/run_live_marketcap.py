from sqlalchemy import create_engine, text

# مشخصات اتصال به دیتابیس رو وارد کن
DATABASE_URL = "postgresql://username:password@localhost:5432/postgres1"
engine = create_engine(DATABASE_URL)

update_sql = text("""
WITH latest_time AS (
    SELECT MAX(updated_at)::date AS target_date FROM live_market_data
),
dollar AS (
    SELECT rate AS dollar_rate
    FROM dollar_data, latest_time
    WHERE date <= target_date
    ORDER BY date DESC
    LIMIT 1
),
prepared AS (
    SELECT
        l.updated_at::date AS date,
        l.stock_ticker,
        l.sector,
        l.value,
        l.market_cap,
        d.dollar_rate,
        (l.market_cap / d.dollar_rate) AS market_cap_usd
    FROM
        live_market_data l,
        latest_time t,
        dollar d
    WHERE
        l.updated_at::date = t.target_date
)
-- حذف داده‌های قبلی روز
DELETE FROM daily_market_summary
WHERE date = (SELECT target_date FROM latest_time);

-- درج داده‌های جدید
INSERT INTO daily_market_summary (date, stock_ticker, sector, value, market_cap, value_usd, market_cap_usd)
SELECT
    date, stock_ticker, sector, value, market_cap, value, market_cap_usd
FROM
    prepared;
""")

with engine.begin() as conn:
    conn.execute(update_sql)

print("✅ daily_market_summary updated with latest live data.")
