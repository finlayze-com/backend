# #استفاده این فایل برای وقتی که کل دیتا بیس تاریخی رو میخوایم دانلود کنیم تو دیتا بیس ذخیره کنیم
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.chrome.options import Options
# from bs4 import BeautifulSoup
# import pandas as pd
# import psycopg2
# import time
#
# # تنظیمات مرورگر (headless)
# options = Options()
# options.add_argument("--headless")
# options.add_argument("--no-sandbox")
# options.add_argument("--disable-dev-shm-usage")
#
# # راه‌اندازی مرورگر
# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
#
# # آدرس سایت
# url = 'https://www.tgju.org/profile/price_dollar_rl/history'
# driver.get(url)
# time.sleep(5)
#
# # پارس HTML بعد از اجرای جاوااسکریپت
# soup = BeautifulSoup(driver.page_source, 'html.parser')
# driver.quit()
#
# # استخراج جدول
# table = soup.find('table', {'id': 'DataTables_Table_0'})
# tbody = table.find('tbody')
#
# # استخراج داده‌ها
# rows = []
# for tr in tbody.find_all('tr'):
#     tds = tr.find_all('td')
#     if len(tds) == 8:
#         open_, low, high, close, *_ , date_gregorian, _ = [td.text.strip().replace(',', '') for td in tds]
#         rows.append((date_gregorian, open_, high, low, close))
#
# # ساخت دیتافریم
# df = pd.DataFrame(rows, columns=['date_miladi', 'open', 'high', 'low', 'close'])
# df['date_miladi'] = pd.to_datetime(df['date_miladi'], format='%Y/%m/%d')
#
# # اتصال به دیتابیس
# conn = psycopg2.connect(
#     host='localhost',
#     dbname='postgres1',
#     user='postgres',
#     password='Afiroozi12'
# )
# cur = conn.cursor()
#
# # گرفتن تاریخ‌هایی که از قبل در دیتابیس هستند
# cur.execute("SELECT date_miladi FROM dollar_data;")
# existing_dates = set(row[0] for row in cur.fetchall())
#
# # فیلتر کردن داده‌هایی که تاریخشان جدید است
# df_new = df[~df['date_miladi'].isin(existing_dates)]
#
# # درج در دیتابیس فقط داده‌های جدید
# insert_query = """
#     INSERT INTO dollar_data (date_miladi, open, high, low, close)
#     VALUES (%s, %s, %s, %s, %s);
# """
# records = list(df_new.itertuples(index=False, name=None))
# if records:
#     cur.executemany(insert_query, records)
#     conn.commit()
#
# print(f"✅ {len(records)} ردیف جدید به جدول dollar_data اضافه شد.")
#
# cur.close()
# conn.close()


# -*- coding: utf-8 -*-
"""
جمع‌آوری تاریخچه نرخ دلار از tgju و ذخیره در Postgres.

- بدون webdriver-manager (حل مشکل CDN)
- استفاده از chromedriver سیستم
- خواندن DB_URL از .env (fallback به پارامترهای ثابت)
- آپسرت روی date_miladi

اجرای پیشنهادی:
    source .venv/bin/activate
    python -m cron_jobs.otherImportantFile.dollar
"""

import os
import sys
import time
import shutil
import traceback
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import psycopg2
from psycopg2.extras import execute_batch


# ---------- تنظیمات عمومی ----------
REPO_ROOT = Path(__file__).resolve().parents[2]  # /root/backend
ENV_PATH = REPO_ROOT / ".env"

URL = "https://www.tgju.org/profile/price_dollar_rl/history"

HEADLESS = True
PAGELOAD_WAIT = 6  # seconds

# ---------- کمک‌تابع‌ها ----------
def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def get_db_url():
    # ترتیب اولویت: DB_URL -> DB_URL_SYNC -> DATABASE_URL
    db_url = os.getenv("DB_URL") or os.getenv("DB_URL_SYNC") or os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    # fallback به اتصال دستی (اگر env نبود)
    host = os.getenv("DB_HOST", "localhost")
    dbname = os.getenv("DB_NAME", "postgres1")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "Afiroozi12")
    port = os.getenv("DB_PORT", "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

def connect_db(db_url: str):
    # psycopg2 با DSN هم کار می‌کند
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    return conn

def get_chromedriver_path() -> str:
    # اول با which
    p = shutil.which("chromedriver")
    if p:
        return p
    # fallback مسیرهای رایج
    candidates = [
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
        "/usr/lib/chromium/chromedriver",
        "/usr/local/bin/chromedriver",
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    raise RuntimeError(
        "chromedriver یافت نشد. لطفاً با دستور زیر نصب کن:\n"
        "  sudo apt-get install -y chromium-driver\n"
        "و سپس دوباره اجرا کن."
    )

def new_driver():
    options = Options()
    if HEADLESS:
        # حالت جدید headless برای نسخه‌های جدید
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,900")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/118.0.0.0 Safari/537.36")

    # اگر نیاز بود باینری کرومیوم را مشخص کن (بعضی سرورها)
    # options.binary_location = "/usr/bin/chromium-browser"  # یا /usr/bin/chromium

    driver_path = get_chromedriver_path()
    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def fetch_html() -> str:
    log(f"در حال باز کردن صفحه: {URL}")
    driver = new_driver()
    try:
        driver.get(URL)
        time.sleep(PAGELOAD_WAIT)
        html = driver.page_source
        return html
    finally:
        try:
            driver.quit()
        except Exception:
            pass

def parse_table(html: str) -> pd.DataFrame:
    """
    جدول با id=DataTables_Table_0 → هشت ستون (باز،کمترین،بیشترین،پایانی،...،تاریخ میلادی،…)
    ساختار ممکن است توسط سایت تغییر کند؛ در صورت تغییر، این بخش را تطبیق بده.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "DataTables_Table_0"})
    if not table:
        raise ValueError("جدول با id=DataTables_Table_0 پیدا نشد. شاید ساختار صفحه تغییر کرده.")

    tbody = table.find("tbody")
    if not tbody:
        raise ValueError("tbody جدول پیدا نشد.")

    rows = []
    for tr in tbody.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        # ساختار معمولاً 8 ستونه است؛ اگر تغییر کرد، لاگ بگیر
        if len(tds) < 7:
            continue

        # معمولاً ترتیب: باز، کمترین، بیشترین، پایانی، ... ، تاریخ میلادی، ...
        # پاکسازی اعداد
        try:
            # حذف ویرگول
            cleaned = [c.replace(",", "") for c in tds]
            # تلاش برای تشخیص تاریخ از ستون‌های انتهایی
            # در بسیاری از نمونه‌ها ستون 6 تاریخ است (ایندکس 6 یا -2)
            # امن‌تر: از راست به چپ اولین چیزی که شبیه تاریخ هست را بردار
            date_idx = None
            for i in range(len(cleaned)-1, -1, -1):
                token = cleaned[i]
                if "/" in token and len(token.split("/")) == 3:
                    date_idx = i
                    break
            if date_idx is None:
                # fallback: فرض 6
                date_idx = 6

            # استخراج تاریخ میلادی
            date_gregorian = cleaned[date_idx]

            # مقادیر قیمت را از ابتدای ردیف بردار (فرض: 0..3)
            open_, low, high, close = cleaned[0:4]

            # تبدیل به float
            def to_float(x):
                try:
                    return float(x)
                except:
                    return None

            rows.append(
                (
                    date_gregorian.strip(),
                    to_float(open_),
                    to_float(high),
                    to_float(low),
                    to_float(close),
                )
            )
        except Exception as e:
            log(f"⚠️ خطا در پارس یک ردیف: {e}")

    if not rows:
        raise ValueError("هیچ سطری از جدول استخراج نشد. شاید صفحه کامل لود نشده یا DOM تغییر کرده.")

    df = pd.DataFrame(rows, columns=["date_miladi", "open", "high", "low", "close"])

    # تاریخ‌ها با فرمت  YYYY/MM/DD
    def parse_date(s):
        # برخی ردیف‌ها ممکن است dash داشته باشند یا خالی باشند
        s = (s or "").strip()
        for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except:
                pass
        return None

    df["date_miladi"] = df["date_miladi"].map(parse_date)
    df = df.dropna(subset=["date_miladi"]).copy()
    return df

def ensure_table(conn):
    """
    اگر جدول dollar_data وجود ندارد، بساز (ستون date_miladi یکتا باشد برای upsert).
    اگر جدول داری، این تابع بی‌خطر است.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS public.dollar_data (
        date_miladi date PRIMARY KEY,
        open numeric,
        high numeric,
        low  numeric,
        close numeric
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def upsert_dollar_data(conn, df: pd.DataFrame):
    """
    آپسرت بر اساس date_miladi.
    اگر constraint یکتا نداری، حتماً PRIMARY KEY یا UNIQUE روی date_miladi بگذار.
    """
    if df.empty:
        log("هیچ داده‌ای برای درج وجود ندارد.")
        return 0

    sql = """
    INSERT INTO public.dollar_data (date_miladi, open, high, low, close)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (date_miladi)
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low  = EXCLUDED.low,
        close= EXCLUDED.close;
    """
    records = [(r.date_miladi, r.open, r.high, r.low, r.close) for r in df.itertuples(index=False)]
    with conn.cursor() as cur:
        execute_batch(cur, sql, records, page_size=1000)
    conn.commit()
    return len(records)

def main():
    try:
        if ENV_PATH.exists():
            load_dotenv(ENV_PATH)
            log(f".env loaded from {ENV_PATH}")
        else:
            log("⚠️ فایل .env پیدا نشد. از متغیرهای محیطی فعلی استفاده می‌کنم.")

        db_url = get_db_url()
        log(f"DB_URL: {db_url}")

        html = fetch_html()
        log("HTML دریافت شد.")

        df = parse_table(html)
        log(f"🧮 ردیف‌های استخراج‌شده: {len(df)}")

        conn = connect_db(db_url)
        try:
            ensure_table(conn)

            # اگر فقط می‌خواهی سطرهای جدید را درج کنی (و نه آپدیت):
            #   - می‌توانی ابتدا تاریخ‌های موجود را بخوانی و فیلتر کنی.
            #   - ولی ما آپسرت می‌کنیم تا مطمئن باشیم مقدارها به‌روز می‌شوند.
            inserted = upsert_dollar_data(conn, df)
            log(f"✅ {inserted} ردیف درج/به‌روزرسانی شد در dollar_data.")
        finally:
            conn.close()

        log("🎉 انجام شد.")

    except Exception as e:
        log("❌ خطا رخ داد:")
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
