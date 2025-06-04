from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import psycopg2
import time
import tempfile

# تنظیمات مرورگر
options = Options()
options.binary_location = "/snap/bin/chromium"
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--remote-debugging-port=9222")
options.add_argument(f"--user-data-dir={tempfile.mkdtemp()}")

# اجرای مرورگر
driver = webdriver.Chrome(
    service=Service("/usr/bin/chromedriver"),
    options=options
)

# باز کردن صفحه و استخراج HTML
url = 'https://www.tgju.org/profile/price_dollar_rl/history'
driver.get(url)
time.sleep(5)
soup = BeautifulSoup(driver.page_source, 'html.parser')
driver.quit()

# استخراج داده‌ها از جدول
table = soup.find('table', {'id': 'DataTables_Table_0'})
tbody = table.find('tbody')

rows = []
for tr in tbody.find_all('tr'):
    tds = tr.find_all('td')
    if len(tds) == 8:
        open_, low, high, close, *_ , date_miladi, _ = [td.text.strip().replace(',', '') for td in tds]
        rows.append((date_miladi, open_, high, low, close))

df = pd.DataFrame(rows, columns=['date_miladi', 'open', 'high', 'low', 'close'])
df['date_miladi'] = pd.to_datetime(df['date_miladi'], format='%Y/%m/%d')

# اتصال به دیتابیس و ثبت داده
conn = psycopg2.connect(
    host='localhost',
    dbname='postgres1',
    user='myuser',
    password='Afiroozi12'
)
cur = conn.cursor()

cur.execute("SELECT date_miladi FROM dollar_data;")
existing_dates = set(row[0] for row in cur.fetchall())

df_new = df[~df['date_miladi'].isin(existing_dates)]

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
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import psycopg2
import time
import tempfile

# تنظیمات مرورگر
options = Options()
options.binary_location = "/snap/bin/chromium"
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--remote-debugging-port=9222")
options.add_argument(f"--user-data-dir={tempfile.mkdtemp()}")

# اجرای مرورگر
driver = webdriver.Chrome(
    service=Service("/usr/bin/chromedriver"),
    options=options
)

# باز کردن صفحه و استخراج HTML
url = 'https://www.tgju.org/profile/price_dollar_rl/history'
driver.get(url)
time.sleep(5)
soup = BeautifulSoup(driver.page_source, 'html.parser')
driver.quit()

# استخراج داده‌ها از جدول
table = soup.find('table', {'id': 'DataTables_Table_0'})
tbody = table.find('tbody')

rows = []
for tr in tbody.find_all('tr'):
    tds = tr.find_all('td')
    if len(tds) == 8:
        open_, low, high, close, *_ , date_miladi, _ = [td.text.strip().replace(',', '') for td in tds]
        rows.append((date_miladi, open_, high, low, close))

df = pd.DataFrame(rows, columns=['date_miladi', 'open', 'high', 'low', 'close'])
df['date_miladi'] = pd.to_datetime(df['date_miladi'], format='%Y/%m/%d')

# اتصال به دیتابیس و ثبت داده
conn = psycopg2.connect(
    host='localhost',
    dbname='postgres1',
    user='myuser',
    password='Afiroozi12'
)
cur = conn.cursor()

cur.execute("SELECT date_miladi FROM dollar_data;")
existing_dates = set(row[0] for row in cur.fetchall())

df_new = df[~df['date_miladi'].isin(existing_dates)]

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
