import psycopg2
import pandas as pd
import logging
from datetime import timedelta
import jdatetime
from sqlalchemy import create_engine, text
from finpy_tse import Get_Queue_History

# ---------------------- تنظیمات لاگ ---------------------- #
logging.basicConfig(
    filename='../../queue_fetch.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# ---------------------- تابع تبدیل تاریخ میلادی به شمسی ---------------------- #
def to_jalali_str(greg_date):
    return jdatetime.date.fromgregorian(date=greg_date).strftime('%Y-%m-%d')


# ---------------------- اتصال به دیتابیس ---------------------- #
try:
    conn = psycopg2.connect(
        dbname="postgres1", user="postgres", password="Afiroozi12",
        host="localhost", port="5432"
    )
    cursor = conn.cursor()
except Exception as e:
    logging.error(f"❌ خطا در اتصال به دیتابیس: {e}")
    raise

# ---------------------- تعیین آخرین روز معاملاتی از daily_stock_data ---------------------- #
try:
    cursor.execute('SELECT MAX("date_miladi") FROM daily_stock_data;')
    last_trading_date = cursor.fetchone()[0]
    if last_trading_date is None:
        raise Exception("هیچ داده‌ای در جدول daily_stock_data وجود ندارد.")
    start_date = to_jalali_str(last_trading_date)
    end_date = to_jalali_str(last_trading_date + timedelta(days=1))
    logging.info(f"📅 دریافت صف خرید/فروش برای تاریخ: {start_date}")
except Exception as e:
    logging.error(f"❌ خطا در گرفتن آخرین روز معاملاتی: {e}")
    conn.close()
    raise

# ---------------------- دریافت لیست نمادها ---------------------- #
try:
    cursor.execute('SELECT "stock_ticker" FROM symboldetail;')
    rows = cursor.fetchall()
    tickers = [row[0] for row in rows]
    logging.info(f"🔍 تعداد نمادها: {len(tickers)}")
except Exception as e:
    logging.error(f"❌ خطا در گرفتن لیست نمادها: {e}")
    conn.close()
    raise

# ---------------------- دریافت صف‌ها ---------------------- #
all_data = []
for ticker in tickers:
    try:
        data = Get_Queue_History(ticker, start_date, end_date)

        if data is None:
            logging.warning(f"{ticker} - ⚠️ خروجی None بود.")
            continue

        if isinstance(data, pd.DataFrame):
            df = data.copy()
        elif isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            logging.warning(f"{ticker} - ⚠️ خروجی ناشناس: {type(data)}")
            continue

        df['stock_ticker'] = ticker
        df['date'] = start_date

        # حذف صف‌هایی که BQ و SQ هر دو صفر هستند
        df = df[(df['BQ_Value'] != 0) | (df['SQ_Value'] != 0)]

        if not df.empty:
            all_data.append(df)
        else:
            logging.info(f"{ticker} - صف‌ها صفر بودن و حذف شد.")

    except Exception as e:
        logging.error(f"{ticker} - ❌ خطا در دریافت داده: {e}")

# ---------------------- پاک کردن داده‌های قبلی + ذخیره‌سازی جدید ---------------------- #
if all_data:
    final_df = pd.concat(all_data)
    final_df = final_df.sort_values(by='BQ_Value', ascending=False)
    logging.info(f"📊 {len(final_df)} ردیف برای ذخیره‌سازی آماده است.")

    try:
        engine = create_engine('postgresql://postgres:Afiroozi12@localhost:5432/postgres1')
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM quote;"))
            logging.info("🗑️ داده‌های قبلی جدول quote حذف شد.")

            final_df.to_sql("quote", con=connection, if_exists='append', index=False)
            logging.info("✅ داده‌های جدید با موفقیت ذخیره شدند.")
    except Exception as e:
        logging.error(f"❌ خطا در ذخیره در دیتابیس: {e}")
else:
    logging.warning("⚠️ هیچ داده معتبری برای ذخیره وجود نداشت.")

# ---------------------- بستن اتصال ---------------------- #
cursor.close()
conn.close()
