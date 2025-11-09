import psycopg2
import pandas as pd
import logging
import jdatetime
from sqlalchemy import create_engine, text
from finpy_tse import Get_Queue_History
from dotenv import load_dotenv
import os
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import MetaData, Table
from datetime import timedelta, datetime, timezone

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


# ---------------------- بارگذاری تنظیمات از .env ---------------------- #
load_dotenv()  # فایل .env را می‌خواند

DB_URL_SYNC = os.getenv("DB_URL_SYNC") or os.getenv("DB_URL")
if not DB_URL_SYNC:
    raise EnvironmentError("❌ متغیر DB_URL یا DB_URL_SYNC در فایل .env یافت نشد.")

# اتصال به دیتابیس با try/except
try:
    conn = psycopg2.connect(DB_URL_SYNC)
    cursor = conn.cursor()
    logging.info("✅ اتصال به دیتابیس برقرار شد.")
except Exception as e:
    logging.error(f"❌ خطا در اتصال به دیتابیس: {e}")
    raise


# ---------------------- تعیین آخرین روز معاملاتی از daily_stock_data ---------------------- #
try:
    cursor.execute('SELECT MAX("Timestamp"::date) FROM orderbook_snapshot;')
    last_trading_date = cursor.fetchone()[0]
    if last_trading_date is None:
        raise Exception("هیچ داده‌ای در جدول orderbook_snapshot وجود ندارد.")
    start_date = to_jalali_str(last_trading_date- timedelta(days=0))
    end_date = to_jalali_str(last_trading_date )
    logging.info(f"📅 دریافت صف خرید/فروش برای تاریخ: {start_date}")
except Exception as e:
    logging.error(f"❌ خطا در گرفتن آخرین روز معاملاتی: {e}")
    conn.close()
    raise

# ---------------------- دریافت لیست نمادها ---------------------- #
try:
    cursor.execute(
        '''
                SELECT DISTINCT "stock_ticker"
                FROM symboldetail
                WHERE "stock_ticker" IS NOT NULL
                  AND instrument_type = 'saham'
                ORDER BY "stock_ticker"
                '''
        )
    rows = cursor.fetchall()
    tickers = [row[0] for row in rows]
    logging.info(f"🔍 تعداد نمادهای سهام (instrument_type='saham'): {len(tickers)}")
except Exception as e:
    logging.error(f"❌ خطا در گرفتن لیست نمادها: {e}")
    conn.close()
    raise

# ---------------------- مپ تیکر → inscode ---------------------- #
try:
    cursor.execute(
        '''
        SELECT "stock_ticker", "insCode"
        FROM symboldetail
        WHERE instrument_type = 'saham'
          AND "stock_ticker" IS NOT NULL
          AND "insCode" IS NOT NULL
        '''
    )
    rows = cursor.fetchall()
    ticker2ins = {row[0]: str(row[1]) for row in rows}  # dict: ticker -> inscode
    logging.info(f"🧩 مپ تیکر→inscode برای {len(ticker2ins)} نماد آماده شد.")
except Exception as e:
    logging.error(f"❌ خطا در گرفتن مپ inscode: {e}")
    conn.close()
    raise

# ---------------------- دریافت صف‌ها ---------------------- #
all_data = []
fetch_ts = datetime.now(timezone.utc)  # زمان ثبت دانلود (timezone-aware)
for ticker in tickers:
    fetch_ts = datetime.now(timezone.utc)  # زمان ثبت دانلود (timezone-aware)

    try:
        ins = ticker2ins.get(ticker)
        if not ins:
            logging.warning(f"{ticker} - inscode یافت نشد، رد شد.")
            continue

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

        # ستون‌های پایه
        df['stock_ticker'] = ticker
        df['inscode'] = ins
        df['downloaded_at'] = fetch_ts  # 👈 زمان دانلود

        # ✅ تاریخ را از J-Date (ستون یا ایندکس) بگیر؛ در غیراینصورت از start_date
        if df.index.name and df.index.name.lower().replace('_','').replace('-','') in ('jdate','jdate'):
            df = df.reset_index().rename(columns={df.index.name: 'date'})
        elif 'J-Date' in df.columns:
            df = df.rename(columns={'J-Date': 'date'})
        else:
            df['date'] = start_date

        # فقط ردیف‌هایی که صف خرید/فروش صفر نیست
        if {'BQ_Value', 'SQ_Value'}.issubset(df.columns):
            df = df[(df['BQ_Value'] != 0) | (df['SQ_Value'] != 0)]
        else:
            logging.warning(f"{ticker} - ستون‌های BQ_Value/SQ_Value موجود نیست.")
            continue

        if not df.empty:
            all_data.append(df)
        else:
            logging.info(f"{ticker} - صف‌ها صفر بودن و حذف شد.")

    except Exception as e:
        logging.error(f"{ticker} - ❌ خطا در دریافت داده: {e}")

# ---------------------- UPSERT ---------------------- #
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    if 'BQ_Value' in final_df.columns:
        final_df = final_df.sort_values(by='BQ_Value', ascending=False)
    final_df = final_df.where(pd.notnull(final_df), None)

    logging.info(f"📊 {len(final_df)} ردیف برای UPSERT آماده است.")

    try:
        engine = create_engine(DB_URL_SYNC)
        # ✅ اگر در DB کلید یکتا را روی (inscode, date) ساخته‌ای:
        conflict_cols = ["inscode", "date"]   # ← توصیه می‌شود
        # اگر هنوز با تیکر کار می‌کنی: conflict_cols = ["stock_ticker", "date"]

        records = final_df.to_dict(orient="records")
        with engine.begin() as connection:
            md = MetaData()
            quote = Table("quote", md, autoload_with=connection)
            update_cols = [c.name for c in quote.columns if c.name not in conflict_cols]

            chunk_size = 1000
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i+chunk_size]
                stmt = pg_insert(quote).values(chunk)
                update_map = {c: getattr(stmt.excluded, c) for c in update_cols}
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_cols,
                    set_=update_map
                )
                connection.execute(upsert_stmt)

        logging.info("✅ UPSERT با موفقیت انجام شد.")
    except Exception as e:
        logging.error(f"❌ خطا در UPSERT در دیتابیس: {e}")
else:
    logging.warning("⚠️ هیچ داده معتبری برای ذخیره وجود نداشت.")

# ---------------------- بستن اتصال ---------------------- #
cursor.close()
conn.close()
