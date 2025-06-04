#استفاده این فایل برای وقتی که کل دیتا بیس تاریخی رو میخوایم دانلود کنیم تو دیتا بیس ذخیره کنیم
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
import time

# تنظیمات مرورگر (headless)
options = Options()
options.binary_location = "/snap/bin/chromium"
options.add_argument("--headless=new")  # headless mode جدیدتر
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--remote-debugging-port=9222")

# راه‌اندازی مرورگر
driver = webdriver.Chrome(
    service=Service("/usr/bin/chromedriver"),
    options=options
)

# آدرس سایت
url = 'https://www.tgju.org/profile/price_dollar_rl/history'
driver.get(url)
time.sleep(5)

# پارس HTML بعد از اجرای جاوااسکریپت
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
    user='myuser',
    password='Afiroozi12'
)
cur = conn.cursor()

# گرفتن تاریخ‌هایی که از قبل در دیتابیس هستند
cur.execute("SELECT date_miladi FROM dollar_data;")
existing_dates = set(row[0] for row in cur.fetchall())

# فیلتر کردن داده‌هایی که تاریخشان جدید است
df_new = df[~df['date_miladi'].isin(existing_dates)]

# درج در دیتابیس فقط داده‌های جدید
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
