import psycopg2
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date

def update_today_dollar():
    # تنظیمات headless مرورگر
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # اجرای مرورگر
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # 📌 بارگذاری صفحه اصلی نرخ دلار
    url = 'https://www.tgju.org/profile/price_dollar_rl'
    driver.get(url)
    time.sleep(3)  # صبر برای لود کامل

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    # 📌 تابع استخراج قیمت‌ها از HTML
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

    # گرفتن نرخ‌ها
    current_price = get_price("نرخ فعلی")
    open_price = get_price("نرخ بازگشایی بازار")
    high_price = get_price("بالاترین قیمت روز")
    low_price = get_price("پایین ترین قیمت روز")
    today = date.today()

    if not all([current_price, open_price, high_price, low_price]):
        print("❌ دریافت نرخ‌ها ناموفق بود.")
        return

    # 📌 اتصال به دیتابیس
    conn = psycopg2.connect(
        host='localhost',
        dbname='postgres1',
        user='postgres',
        password='Afiroozi12'
    )
    cur = conn.cursor()

    # حذف داده قبلی برای تاریخ امروز
    cur.execute("DELETE FROM dollar_data WHERE date_miladi = %s;", (today,))

    # افزودن داده جدید
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

# اجرای تابع
if __name__ == "__main__":
    update_today_dollar()
