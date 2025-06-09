import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
import datetime
import sys
sys.stdout.reconfigure(encoding='utf-8')

def update_weekly_data():
    # بارگذاری متغیرهای محیطی
    dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
    load_dotenv(dotenv_path)
    print("✅ Loaded DB_URL:", os.getenv("DB_URL"))


    conn = psycopg2.connect(os.getenv("DB_URL"))
    cur = conn.cursor()

    # آخرین هفته ذخیره شده
    cur.execute("SELECT MAX(week_end) FROM weekly_stock_data")
    last_date = cur.fetchone()[0]
    last_date = pd.to_datetime(last_date) if last_date else pd.Timestamp("1900-01-01")

    today = pd.Timestamp.today().normalize()

    df = pd.read_sql("SELECT * FROM daily_stock_data", engine)
    df['date_miladi'] = pd.to_datetime(df['date_miladi'])

    # دریافت نرخ دلار
    dollar_df = pd.read_sql("SELECT date_miladi, close FROM dollar_data ORDER BY date_miladi", engine)
    dollar_df.rename(columns={'close': 'dollar_rate'}, inplace=True)
    dollar_df['date_miladi'] = pd.to_datetime(dollar_df['date_miladi'])
    dollar_df = dollar_df.set_index('date_miladi').sort_index()

    symbols = df['stock_ticker'].unique()
    all_weekly = []

    for symbol in symbols:
        data = df[df['stock_ticker'] == symbol].copy()
        data.set_index('date_miladi', inplace=True)

        weekly = data.groupby(pd.Grouper(freq='W-FRI', label='left', closed='left')).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'final_price': 'last',
            'adjust_open': 'first',
            'adjust_high': 'max',
            'adjust_low': 'min',
            'adjust_close': 'last',
            'adjust_final_price': 'last',
            'volume': 'sum',
            'value': 'sum'
        }).reset_index()

        weekly.dropna(subset=['open', 'high', 'low', 'close', 'final_price'], inplace=True)

        weekly['week_start'] = weekly['date_miladi']
        weekly['week_end'] = weekly['week_start'] + pd.Timedelta(days=6)
        weekly['stock_ticker'] = symbol
        weekly['name'] = data['name'].iloc[0]
        weekly['market'] = data['market'].iloc[0]

        # فقط هفته‌های جدید کامل‌شده
        weekly = weekly[(weekly['week_end'] > last_date)]

        # نگاشت نرخ دلار
        weekly['dollar_rate'] = weekly['week_start'].apply(
            lambda x: dollar_df.loc[:x].iloc[-1]['dollar_rate'] if not dollar_df.loc[:x].empty else None
        )
        weekly['dollar_rate'] = pd.to_numeric(weekly['dollar_rate'], errors='coerce')
        weekly = weekly.dropna(subset=['dollar_rate'])

        # محاسبه ستون‌های دلاری
        weekly['adjust_open_usd'] = weekly['adjust_open'] / weekly['dollar_rate']
        weekly['adjust_high_usd'] = weekly['adjust_high'] / weekly['dollar_rate']
        weekly['adjust_low_usd'] = weekly['adjust_low'] / weekly['dollar_rate']
        weekly['adjust_close_usd'] = weekly['adjust_close'] / weekly['dollar_rate']
        weekly['value_usd'] = weekly['value'] / weekly['dollar_rate']

        weekly = weekly[[
            'stock_ticker', 'week_start', 'week_end',
            'open', 'high', 'low', 'close', 'final_price',
            'adjust_open', 'adjust_high', 'adjust_low', 'adjust_close', 'adjust_final_price',
            'volume', 'value',
            'name', 'market',
            'dollar_rate', 'adjust_open_usd', 'adjust_high_usd',
            'adjust_low_usd', 'adjust_close_usd', 'value_usd'
        ]]

        all_weekly.append(weekly)

    if not all_weekly:
        print("📭 هفته جدیدی برای ذخیره نیست.")
    else:
        final_weekly_df = pd.concat(all_weekly, ignore_index=True)

        insert_query = """
        INSERT INTO weekly_stock_data (
            stock_ticker, week_start, week_end,
            open, high, low, close, final_price,
            adjust_open, adjust_high, adjust_low, adjust_close, adjust_final_price,
            volume, value,
            name, market,
            dollar_rate, adjust_open_usd, adjust_high_usd,
            adjust_low_usd, adjust_close_usd, value_usd
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_ticker, week_end) DO NOTHING;
        """

        with conn.cursor() as cur:
            cur.executemany(insert_query, final_weekly_df.values.tolist())
            conn.commit()
            print(f"✅ {len(final_weekly_df)} هفته جدید با نرخ دلار ذخیره شد.")

    cur.close()
    conn.close()

# اجرای تابع
# ✅ فقط این قسمت جدید اضافه شده:
if __name__ == "__main__":
    update_weekly_data()
