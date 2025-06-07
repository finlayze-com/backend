import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import jdatetime
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

    # اتصال امن به دیتابیس
    print("✅ Loaded DB_URL:", os.getenv("DB_URL"))

    conn = psycopg2.connect(os.getenv("DB_URL"))
    cur = conn.cursor()
    # تعریف total_rows
    total_rows = 0

    cur.execute("""
        SELECT stock_ticker 
        FROM symbolDetail 
        WHERE panel NOT IN (
            'بازار ابزارهای نوین مالی',
            'بازار ابزارهاي نوين مالي فرابورس',
            'بازار اوراق بدهی',
            'بازار سایر اوراق بهادار قابل معامله'
            
        )
        AND panel IS NOT NULL
        ;
    """)
    stock_list = [row[0] for row in cur.fetchall()]

    dollar_df = pd.read_sql("SELECT date_miladi, close FROM dollar_data", conn)
    dollar_df.rename(columns={'close': 'dollar_rate'}, inplace=True)
    dollar_df['date_miladi'] = pd.to_datetime(dollar_df['date_miladi'])

    for stock in stock_list:
        print(f"🔍 در حال بررسی نماد: {stock}")
        try:
            # گرفتن آخرین تاریخ برای نماد
            cur.execute("SELECT MAX(date_miladi) FROM daily_stock_data WHERE stock_ticker = %s", (stock,))
            last_date = cur.fetchone()[0]

            # حذف سطر آخر اگر is_temp=True باشد
            if last_date:
                cur.execute("""
                    DELETE FROM daily_stock_data
                    WHERE stock_ticker = %s AND date_miladi = %s AND (is_temp IS TRUE OR is_temp = True);
                """, (stock, last_date))
                conn.commit()

            # دریافت داده‌ها از API
            df = fps.Get_Price_History(stock=stock, ignore_date=True, adjust_price=True, show_weekday=True)
            if df is None or df.empty:
                print(f"⚠️ داده‌ای برای نماد {stock} موجود نیست.")
                continue

            df['gregorian_date'] = df.index.astype(str).map(convert_jalali_to_gregorian)
            df['j_date'] = df.index.astype(str)
            df['gregorian_date'] = pd.to_datetime(df['gregorian_date'])

            #if last_date:
                #df = df[df['gregorian_date'] >= pd.to_datetime(last_date)]

            # if df.empty:
            #     print(f"📭 داده جدیدی برای {stock} وجود ندارد.")
            #     continue

            df = df.merge(dollar_df, how='left', left_on='gregorian_date', right_on='date_miladi')
            df['dollar_rate'] = df['dollar_rate'].fillna(method='ffill')

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
            print(f"📊 تعداد ردیف‌های دریافتی برای {stock}: {len(df)}")
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
            total_rows += len(df)
            print(f"✅ {stock} | {len(df)} ردیف جدید ذخیره شد.")

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"❌ خطا در پردازش {stock}: {e}")

    cur.close()
    conn.close()

    if total_rows > 0:
        print("📦 ذخیره‌سازی داده‌های روزانه کامل شد.")
    else:
        print("⚠️ هیچ داده‌ای ذخیره نشد.")


# اجرای تابع اصلی
# ✅ فقط این قسمت جدید اضافه شده:
if __name__ == "__main__":
    update_daily_data()
