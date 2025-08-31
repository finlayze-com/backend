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
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, ElementClickInterceptedException, JavascriptException
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

import psycopg2
from psycopg2.extras import execute_batch


# ---------- تنظیمات عمومی ----------
REPO_ROOT = Path(__file__).resolve().parents[2]  # /root/backend
ENV_PATH = REPO_ROOT / ".env"

URL = "https://www.tgju.org/profile/price_dollar_rl/history"

HEADLESS = True
PAGELOAD_WAIT = 6   # seconds
WAIT_TIMEOUT = 20   # WebDriverWait seconds


# ---------- DB helpers ----------
def normalize_psycopg2_url(url: str) -> str:
    """psycopg2 فقط postgresql:// را می‌شناسد."""
    return (
        url.replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgresql+psycopg2://", "postgresql://")
    )

def get_db_url():
    # اولویت: sync → async
    db_url = os.getenv("DB_URL_SYNC") or os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        host = os.getenv("DB_HOST", "localhost")
        dbname = os.getenv("DB_NAME", "postgres1")
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD", "Afiroozi12")
        port = os.getenv("DB_PORT", "5432")
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return normalize_psycopg2_url(db_url)

def connect_db(db_url: str):
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    return conn


# ---------- سایر کمک‌تابع‌ها ----------
def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def get_chromedriver_path() -> str:
    p = shutil.which("chromedriver")
    if p:
        return p
    for c in ["/usr/bin/chromedriver",
              "/usr/lib/chromium-browser/chromedriver",
              "/usr/lib/chromium/chromedriver",
              "/usr/local/bin/chromedriver"]:
        if os.path.exists(c):
            return c
    raise RuntimeError(
        "chromedriver یافت نشد. نصب کن:\n"
        "  sudo apt-get install -y chromium-driver\n"
    )

def new_driver():
    options = Options()
    if HEADLESS:
        try:
            options.add_argument("--headless=new")
        except Exception:
            options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,900")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/118.0.0.0 Safari/537.36")
    # options.binary_location = "/usr/bin/chromium-browser"  # در صورت نیاز

    service = Service(executable_path=get_chromedriver_path())
    return webdriver.Chrome(service=service, options=options)

def dismiss_overlays(driver):
    """بنرها/اوورلی‌های مزاحم (footer, toast, modal, info-price, ads) را حذف/مخفی می‌کند."""
    js = """
    (function(){
      const sel = [
        '.fixed-bottom', '.fixed-top', '.sticky', '.toast', '.modal', '.modal-backdrop',
        '#cookie', '#cookies', '.cookie', '.cookie-consent', '.ad', '[id*="ad"]',
        '.navbar-fixed-bottom', '.footer', '.info-price'
      ];
      const nodes = [];
      sel.forEach(s => document.querySelectorAll(s).forEach(el => nodes.push(el)));
      nodes.forEach(el => { try { el.style.display='none'; el.remove(); } catch(e){} });
    })();
    """
    try:
        driver.execute_script(js)
    except JavascriptException:
        pass

def js_click(driver, element):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        time.sleep(0.1)
        driver.execute_script("arguments[0].click();", element)
        return True
    except Exception:
        return False

def parse_visible_rows_from_dom(html: str):
    """ردیف‌های صفحهٔ فعلی جدول را از DOM پارس می‌کند."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "DataTables_Table_0"})
    if not table:
        return []
    tbody = table.find("tbody")
    if not tbody:
        return []
    rows = []
    for tr in tbody.find_all("tr"):
        tds = [td.get_text(strip=True).replace(",", "") for td in tr.find_all("td")]
        if len(tds) < 7:
            continue

        # تاریخ را از راست‌ترین ستون شبیه تاریخ بردار
        date_idx = None
        for i in range(len(tds)-1, -1, -1):
            token = tds[i]
            if "/" in token and len(token.split("/")) == 3:
                date_idx = i
                break
        if date_idx is None:
            date_idx = 6  # fallback

        date_gregorian = tds[date_idx]
        open_, low, high, close = tds[0:4]

        def to_float(x):
            try:
                return float(x)
            except:
                return None

        rows.append((
            date_gregorian.strip(),
            to_float(open_), to_float(high), to_float(low), to_float(close),
        ))
    return rows

def fetch_all_rows() -> list[tuple]:
    """همه صفحات DataTables را پیمایش و همهٔ ردیف‌ها را برمی‌گرداند."""
    driver = new_driver()
    try:
        driver.get(URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        # صبر تا جدول لود شود
        wait.until(EC.presence_of_element_located((By.ID, "DataTables_Table_0")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#DataTables_Table_0 tbody tr")))

        # افزایش طول صفحه (اگر select وجود داشته باشد)
        try:
            length_select = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#DataTables_Table_0_length select")))
            sel = Select(length_select)
            try:
                sel.select_by_visible_text("100")
            except Exception:
                # اگر '100' نبود، بزرگ‌ترین گزینه (ممکن است 'All' باشد)
                try:
                    sel.select_by_index(len(sel.options)-1)
                except Exception:
                    pass
            time.sleep(0.8)
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#DataTables_Table_0 tbody tr")))
        except Exception:
            pass  # اگر نبود، ادامه

        all_rows = []
        # صفحهٔ اول
        all_rows.extend(parse_visible_rows_from_dom(driver.page_source))

        # 2) پیمایش صفحه‌ها با کلیک Next (با مقاومت در برابر اوورلی)
        while True:
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, "#DataTables_Table_0_next")
            except Exception:
                break

            classes = (next_btn.get_attribute("class") or "")
            if "disabled" in classes:
                break

            # متن اولین سلول قبل از کلیک برای تشخیص تغییر
            try:
                any_row = driver.find_element(By.CSS_SELECTOR, "#DataTables_Table_0 tbody tr")
                first_cell_before = any_row.find_element(By.CSS_SELECTOR, "td").text
            except Exception:
                any_row = None
                first_cell_before = None

            clicked = False
            for attempt in range(4):
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
                    time.sleep(0.1)
                    next_btn.click()
                    clicked = True
                    break
                except ElementClickInterceptedException:
                    dismiss_overlays(driver)
                    if js_click(driver, next_btn):
                        clicked = True
                        break
                    time.sleep(0.2)
                except WebDriverException:
                    # احتمال تغییر DOM
                    try:
                        next_btn = driver.find_element(By.CSS_SELECTOR, "#DataTables_Table_0_next")
                    except Exception:
                        break

            if not clicked:
                # نتوانستیم کلیک کنیم
                break

            # صبر تا داده‌ها واقعاً تغییر کنند
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#DataTables_Table_0 tbody tr")))
                if first_cell_before:
                    for _ in range(50):  # ~5s
                        try:
                            first_cell_after = driver.find_element(By.CSS_SELECTOR, "#DataTables_Table_0 tbody tr td").text
                        except Exception:
                            first_cell_after = None
                        if first_cell_after and first_cell_after != first_cell_before:
                            break
                        time.sleep(0.1)
            except Exception:
                pass

            # اضافه کردن سطرهای صفحهٔ جدید
            all_rows.extend(parse_visible_rows_from_dom(driver.page_source))

        return all_rows

    finally:
        try:
            driver.quit()
        except Exception:
            pass

def parse_all_pages_to_df() -> pd.DataFrame:
    rows = fetch_all_rows()
    if not rows:
        raise ValueError("هیچ سطری از جدول استخراج نشد.")
    df = pd.DataFrame(rows, columns=["date_miladi", "open", "high", "low", "close"])

    def parse_date(s):
        s = (s or "").strip()
        for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except:
                pass
        return None

    df["date_miladi"] = df["date_miladi"].map(parse_date)
    df = df.dropna(subset=["date_miladi"]).copy()
    # حذف دوبل احتمالی
    df = df.drop_duplicates(subset=["date_miladi"], keep="first")
    return df

def ensure_table(conn):
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
            log("⚠️ .env پیدا نشد. از env فعلی استفاده می‌کنم.")

        print("RAW DB_URL     =", repr(os.getenv("DB_URL")))
        print("RAW DB_URL_SYNC=", repr(os.getenv("DB_URL_SYNC")))

        db_url = get_db_url()
        log(f"EFFECTIVE DB_URL (psycopg2) = {db_url}")

        log("در حال استخراج همه صفحات…")
        df = parse_all_pages_to_df()
        log(f"🧮 کل ردیف‌های استخراج‌شده: {len(df)}")

        conn = connect_db(db_url)
        try:
            ensure_table(conn)
            inserted = upsert_dollar_data(conn, df)
            log(f"✅ {inserted} ردیف درج/به‌روزرسانی شد در dollar_data.")
        finally:
            conn.close()

        log("🎉 انجام شد.")
    except Exception:
        log("❌ خطا رخ داد:")
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()