import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2

# تنظیمات اتصال به پایگاه داده
DB_URI = "postgresql://postgres:Afiroozi12@localhost:5432/postgres1"
engine = create_engine(DB_URI)
conn = psycopg2.connect(host="localhost", dbname="postgres1", user="postgres", password="Afiroozi12")
cur = conn.cursor()

# 1. بارگذاری داده لایو و گرفتن آخرین رکورد از هر نماد
live_df = pd.read_sql("SELECT * FROM live_market_data", engine)
live_df['updated_at'] = pd.to_datetime(live_df['updated_at'])
live_df = live_df.sort_values('updated_at').drop_duplicates('Ticker', keep='last')

# 2. ساخت تاریخ میلادی و هفته
live_df['date_miladi'] = live_df['updated_at'].dt.date
live_df['weekday'] = live_df['updated_at'].dt.day_name().str[:10]  # حداکثر ۱۰ حرف

# 3. گرفتن نرخ دلار
dollar_df = pd.read_sql("SELECT date_miladi, close FROM dollar_data", engine)
dollar_df['date_miladi'] = pd.to_datetime(dollar_df['date_miladi'])
dollar_df = dollar_df.set_index('date_miladi').sort_index()

def get_nearest_dollar(date):
    date = pd.to_datetime(date)
    before = dollar_df.loc[:date]
    return before.iloc[-1]['close'] if not before.empty else None

live_df['dollar_rate'] = live_df['date_miladi'].apply(get_nearest_dollar)

# 4. محاسبه ستون‌های تعدیل‌شده و دلاری
for col in ['Open', 'High', 'Low', 'Close', 'Final', 'Value']:
    live_df[f'adjust_{col.lower()}'] = live_df[col]
    live_df[f'{col.lower()}_usd'] = live_df[col] / live_df['dollar_rate']

live_df['adjust_final_price'] = live_df['Final']

# 5. اتصال به جدول نمادها
symbol_df = pd.read_sql("SELECT stock_ticker, name, market FROM symbolDetail", engine)
live_df = live_df.merge(symbol_df, left_on='Ticker', right_on='stock_ticker', how='left', suffixes=('', '_right'))

# 6. حذف داده‌های موقت قبلی
cur.execute("DELETE FROM daily_stock_data WHERE is_temp = TRUE;")

# 7. ساخت رکوردها
records = []
for i, row in live_df.iterrows():
    j_date_str = row['Download'].strftime('%Y/%m/%d') if isinstance(row['Download'], (datetime, pd.Timestamp)) else str(row['Download'])[:10]
    records.append((
        row['Ticker'], j_date_str, row['date_miladi'], row['weekday'],
        row['Open'], row['High'], row['Low'], row['Close'], row['Final'],
        row['Volume'], row['Value'], row['No'], row['Name'], row['market'],
        row['adjust_open'], row['adjust_high'], row['adjust_low'], row['adjust_close'], row['adjust_final_price'],
        row['dollar_rate'], row['open_usd'], row['high_usd'], row['low_usd'],
        row['close_usd'], row['value_usd'], True
    ))

# 8. اجرای درج یا بروزرسانی
insert_query = """
INSERT INTO daily_stock_data (
    stock_ticker, j_date, date_miladi, weekday, open, high, low, close, final_price,
    volume, value, trade_count, name, market,
    adjust_open, adjust_high, adjust_low, adjust_close, adjust_final_price,
    dollar_rate, adjust_open_usd, adjust_high_usd, adjust_low_usd, adjust_close_usd, value_usd,
    is_temp
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
ON CONFLICT (stock_ticker, date_miladi) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
    final_price = EXCLUDED.final_price, volume = EXCLUDED.volume, value = EXCLUDED.value,
    trade_count = EXCLUDED.trade_count, adjust_open = EXCLUDED.adjust_open,
    adjust_high = EXCLUDED.adjust_high, adjust_low = EXCLUDED.adjust_low,
    adjust_close = EXCLUDED.adjust_close, adjust_final_price = EXCLUDED.adjust_final_price,
    dollar_rate = EXCLUDED.dollar_rate, adjust_open_usd = EXCLUDED.adjust_open_usd,
    adjust_high_usd = EXCLUDED.adjust_high_usd, adjust_low_usd = EXCLUDED.adjust_low_usd,
    adjust_close_usd = EXCLUDED.adjust_close_usd, value_usd = EXCLUDED.value_usd,
    is_temp = EXCLUDED.is_temp;
"""

cur.executemany(insert_query, records)
conn.commit()
cur.close()
conn.close()
print("✅ داده‌های لایو به daily_stock_data منتقل شد.")
