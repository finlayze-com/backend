import psycopg2
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date
import os
from dotenv import load_dotenv

# بارگذاری متغیرهای .env
load_dotenv()

def update_today_dollar():
    # تنظیمات headless مرورگر
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # اجرای مرورگر
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # بارگذاری صفحه نرخ دلار
    url = 'https://www.tgju.org/profile/price_dollar_rl'
    driver.get(url)
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    def get_price(label):
        try:
            rows = soup.select('tbody.table-padding-lg tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) == 2 and cells[0].text.strip() == label:
                    return cells[1].text.strip().replace(',', '')
        except Exception as e:
            print(f"❌ خطا در دریافت {label}: {e}")
        return None

    current_price = get_price("نرخ فعلی")
    open_price = get_price("نرخ بازگشایی بازار")
    high_price = get_price("بالاترین قیمت روز")
    low_price = get_price("پایین ترین قیمت روز")
    today = date.today()

    if not all([current_price, open_price, high_price, low_price]):
        print("❌ دریافت نرخ‌ها ناموفق بود (داده None).")
        return

    if "-" in [current_price, open_price, high_price, low_price]:
        print("⚠️ دریافت مقدار نامعتبر از سایت (علامت -)، ذخیره انجام نشد.")
        return

    try:
        current_price = float(current_price)
        open_price = float(open_price)
        high_price = float(high_price)
        low_price = float(low_price)
    except ValueError:
        print("❌ خطا در تبدیل نرخ‌ها به عدد (float)، ذخیره انجام نشد.")
        return

    # اتصال امن به دیتابیس از .env
    conn = psycopg2.connect(os.getenv("DB_URL"))
    cur = conn.cursor()

    cur.execute("DELETE FROM dollar_data WHERE date_miladi = %s;", (today,))

    cur.execute("""
        INSERT INTO dollar_data (date_miladi, open, high, low, close)
        VALUES (%s, %s, %s, %s, %s);
    """, (
        today,
        open_price,
        high_price,
        low_price,
        current_price
    ))

    conn.commit()
    print(f"✅ نرخ دلار برای {today} با موفقیت ذخیره شد.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    update_today_dollar()
