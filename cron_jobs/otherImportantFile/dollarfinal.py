from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import psycopg2
import time
import tempfile

# # تنظیمات مرورگر (headless)
# options = Options()
# #options.binary_location = "/usr/bin/google-chrome"
# options.add_argument("--headless=new")
# options.add_argument("--no-sandbox")
# options.add_argument("--disable-dev-shm-usage")
# options.add_argument("--disable-gpu")
# options.add_argument("--remote-debugging-port=9222")
#
# # جلوگیری از conflict در user-data-dir
# options.add_argument(f'--user-data-dir={tempfile.mkdtemp()}')
#
# # راه‌اندازی مرورگر با chromedriver سیستم
# driver = webdriver.Chrome(
#     service=Service("/usr/local/bin/chromedriver"),
#     options=options
# )

import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

chrome_options = Options()
chrome_options.add_argument("--headless=new")           # برای سرور
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")

# اگر مسیر باینری Chrome را از قبل داری، از ENV بخوان (برای سرور لینوکسی مفیده)
chrome_bin = os.getenv("CHROME_BIN")
if chrome_bin:
    chrome_options.binary_location = chrome_bin

# درایور را خودکار دانلود و ست می‌کند (روی ویندوز/لینوکس)
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# آدرس سایت
url = 'https://www.tgju.org/profile/price_dollar_rl/history'
driver.get(url)
time.sleep(5)

# پارس HTML
soup = BeautifulSoup(driver.page_source, 'html.parser')
driver.quit()

# استخراج جدول
table = soup.find('table', {'id': 'DataTables_Table_0'})
tbody = table.find('tbody')

# استخراج داده‌ها
rows = []
for tr in tbody.find_all('tr'):
    tds = tr.find_all('td')
    if len(tds) == 8:
        open_, low, high, close, *_ , date_gregorian, _ = [td.text.strip().replace(',', '') for td in tds]
        rows.append((date_gregorian, open_, high, low, close))

# ساخت دیتافریم
df = pd.DataFrame(rows, columns=['date_miladi', 'open', 'high', 'low', 'close'])
df['date_miladi'] = pd.to_datetime(df['date_miladi'], format='%Y/%m/%d')

# اتصال به دیتابیس
conn = psycopg2.connect(
    host='localhost',
    dbname='postgres1',
    user='postgres',
    password='Afiroozi12'
)
cur = conn.cursor()

# گرفتن تاریخ‌هایی که قبلاً ذخیره شده‌اند
cur.execute("SELECT date_miladi FROM dollar_data;")
existing_dates = set(row[0] for row in cur.fetchall())

# فقط داده‌های جدید
df_new = df[~df['date_miladi'].isin(existing_dates)]

# درج
insert_query = """
    INSERT INTO dollar_data (date_miladi, open, high, low, close)
    VALUES (%s, %s, %s, %s, %s);
"""
records = list(df_new.itertuples(index=False, name=None))
if records:
    cur.executemany(insert_query, records)
    conn.commit()

print(f"✅ {len(records)} ردیف جدید به جدول dollar_data اضافه شد.")

cur.close()
conn.close()
