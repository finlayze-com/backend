import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import jdatetime
import traceback
import finpy_tse as fps

def convert_jalali_to_gregorian(jalali_date):
    try:
        year, month, day = map(int, jalali_date.split('-'))
        gregorian_date = jdatetime.date(year, month, day).togregorian()
        return gregorian_date.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"❌ خطا در تبدیل تاریخ {jalali_date}: {e}")
        return None

def update_daily_data():
    # بارگذاری فایل .env
    dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
    load_dotenv(dotenv_path)

    DB_URL = os.getenv("DB_URL")
    print("✅ Loaded DB_URL:", DB_URL)

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # گرفتن لیست نمادها
    cur.execute("""
        SELECT stock_ticker 
        FROM symbolDetail 
        WHERE panel NOT IN (
            'بازار ابزارهای نوین مالی',
            'بازار ابزارهاي نوين مالي فرابورس',
            'بازار اوراق بدهی',
            'بازار سایر اوراق بهادار قابل معامله'
        )
        AND panel IS NOT NULL;
    """)
    stock_list = [row[0] for row in cur.fetchall()]
    print(f"📋 تعداد نمادها: {len(stock_list)}")

    # نرخ دلار
    dollar_df = pd.read_sql("SELECT date_miladi, close FROM dollar_data", conn)
    dollar_df.rename(columns={'close': 'dollar_rate'}, inplace=True)
    dollar_df['date_miladi'] = pd.to_datetime(dollar_df['date_miladi'])

    total_inserted = 0

    for stock in stock_list:
        print(f"🔍 بررسی نماد: {stock}")
        try:
            # آخرین تاریخ ذخیره‌شده
            cur.execute("SELECT MAX(date_miladi) FROM daily_stock_data WHERE stock_ticker = %s", (stock,))
            last_date = cur.fetchone()[0]
            print(f"📆 آخرین تاریخ موجود: {last_date}")

            if last_date:
                cur.execute("""
                    DELETE FROM daily_stock_data
                    WHERE stock_ticker = %s AND date_miladi = %s AND (is_temp IS TRUE OR is_temp = True);
                """, (stock, last_date))
                conn.commit()

            df = fps.Get_Price_History(stock=stock, ignore_date=True, adjust_price=True, show_weekday=True)
            if df is None or df.empty:
                print(f"⚠️ داده‌ای دریافت نشد برای {stock}")
                continue

            print(f"📄 نمونه داده‌ها برای {stock}:\n{df.head()}")

            df['gregorian_date'] = df.index.astype(str).map(convert_jalali_to_gregorian)
            df['j_date'] = df.index.astype(str)
            df['gregorian_date'] = pd.to_datetime(df['gregorian_date'])

            df = df.merge(dollar_df, how='left', left_on='gregorian_date', right_on='date_miladi')
            df['dollar_rate'] = df['dollar_rate'].fillna(method='ffill')

            print(df[['gregorian_date', 'dollar_rate']].tail(5))

            df['adjust_open_usd'] = df['Adj Open'] / df['dollar_rate']
            df['adjust_high_usd'] = df['Adj High'] / df['dollar_rate']
            df['adjust_low_usd'] = df['Adj Low'] / df['dollar_rate']
            df['adjust_close_usd'] = df['Adj Close'] / df['dollar_rate']
            df['value_usd'] = df['Value'] / df['dollar_rate']

            records = list(zip(
                [stock] * len(df), df['j_date'], df['gregorian_date'], df['Weekday'], df['Open'], df['High'], df['Low'],
                df['Close'], df['Final'], df['Volume'], df['Value'], df['No'], df['Name'], df['Market'],
                df['Adj Open'], df['Adj High'], df['Adj Low'], df['Adj Close'], df['Adj Final'],
                df['dollar_rate'], df['adjust_open_usd'], df['adjust_high_usd'], df['adjust_low_usd'],
                df['adjust_close_usd'], df['value_usd']
            ))
            print(f"📊 {stock} | ردیف‌های قابل ذخیره: {len(records)}")

            insert_query = """
                INSERT INTO daily_stock_data (
                    stock_ticker, j_date, date_miladi, weekday, open, high, low, close, final_price, volume, value, trade_count,
                    name, market, adjust_open, adjust_high, adjust_low, adjust_close, adjust_final_price,
                    dollar_rate, adjust_open_usd, adjust_high_usd, adjust_low_usd, adjust_close_usd, value_usd
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_ticker, date_miladi) DO NOTHING;
            """

            cur.executemany(insert_query, records)
            conn.commit()
            total_inserted += len(records)
            print(f"✅ ذخیره شد: {stock} | {len(records)} ردیف")

        except Exception as e:
            print(f"❌ خطا در {stock}: {e}")
            traceback.print_exc()

    cur.close()
    conn.close()

    if total_inserted > 0:
        print(f"📦 ذخیره‌سازی داده‌های روزانه کامل شد: {total_inserted} ردیف ذخیره شد.")
    else:
        print("⚠️ هیچ داده‌ای ذخیره نشد.")

if __name__ == "__main__":
    update_daily_data()
