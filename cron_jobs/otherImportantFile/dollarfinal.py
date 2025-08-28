import os, time, tempfile
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# -------------------- تنظیمات کلی --------------------
load_dotenv()
URL = "https://www.tgju.org/profile/price_dollar_rl/history"
TABLE_ID = "DataTables_Table_0"

# -------------------- Driver --------------------
def build_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(f'--user-data-dir={tempfile.mkdtemp()}')
    # اگر روی سرور مسیر باینری Chrome داری
    if os.getenv("CHROME_BIN"):
        chrome_options.binary_location = os.getenv("CHROME_BIN")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

# -------------------- Scrape helpers --------------------
def set_page_length_100(driver):
    """اندازه صفحه DataTables را تا 100 ردیف بالا می‌برد (اگر موجود باشد)."""
    try:
        length_sel = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, f"{TABLE_ID}_length"))
        )
        Select(length_sel).select_by_value("100")
        # صبر برای رندر مجدد
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, f"#{TABLE_ID} tbody tr"))
        )
        time.sleep(0.3)
    except Exception:
        # اگر سِلِکتور نبود، با همان پیش‌فرض (۳۰ تایی) ادامه می‌دهیم
        pass

def parse_current_page(driver):
    """خروجی: list[tuple(date, open, high, low, close)] از همین صفحه"""
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.find("table", {"id": TABLE_ID})
    tbody = table.find("tbody")
    out = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) == 8:
            open_, low, high, close, *_ , date_gregorian, _ = [
                td.text.strip().replace(",", "") for td in tds
            ]
            out.append((date_gregorian, open_, high, low, close))
    return out

def safe_click_next(driver):
    """روی دکمهٔ بعدی کلیک می‌کند؛ اگر عنصر دیگری کلیک را گرفت، با JS کلیک می‌کند."""
    next_btn = driver.find_element(By.ID, f"{TABLE_ID}_next")
    if "disabled" in next_btn.get_attribute("class"):
        return False

    # اسکرول تا دکمه در دید باشد
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
    time.sleep(0.1)

    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, f"{TABLE_ID}_next"))
        )
        next_btn.click()
    except (ElementClickInterceptedException, TimeoutException):
        # اگر چیزی روی دکمه افتاد، با JS کلیک کن
        driver.execute_script("arguments[0].click();", next_btn)
    return True

def scrape_all_pages():
    driver = build_driver()
    driver.get(URL)

    # صبر تا جدول بیاید
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, TABLE_ID)))
    # اندازه صفحه را زیاد کن تا تعداد صفحات کم شود
    set_page_length_100(driver)

    all_rows = []

    # خواندن اولین صفحه
    all_rows.extend(parse_current_page(driver))

    while True:
        # نشانهٔ تغییر صفحه: متن اولین ردیف
        try:
            first_row_el = driver.find_element(By.CSS_SELECTOR, f"#{TABLE_ID} tbody tr:first-child")
            first_text = first_row_el.text
        except Exception:
            first_text = None

        progressed = safe_click_next(driver)
        if not progressed:
            break

        # صبر تا ردیف اول تغییر کند (صفحه عوض شود)
        try:
            WebDriverWait(driver, 20).until(
                lambda d: d.find_element(By.CSS_SELECTOR, f"#{TABLE_ID} tbody tr:first-child").text != first_text
            )
        except TimeoutException:
            time.sleep(0.5)

        time.sleep(0.2)
        all_rows.extend(parse_current_page(driver))

    driver.quit()
    return all_rows

# -------------------- DataFrame & DB --------------------
def to_dataframe(rows):
    df = pd.DataFrame(rows, columns=["date_miladi", "open", "high", "low", "close"])
    df["date_miladi"] = pd.to_datetime(df["date_miladi"], format="%Y/%m/%d")
    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date_miladi", "open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["date_miladi"]).sort_values("date_miladi")
    return df

def upsert_to_db(df):
    conn = psycopg2.connect(os.getenv("DB_URL_SYNC"))
    cur = conn.cursor()

    # پیشنهاد: یک‌بار در DB اجرا کن تا آپسرت کار کند:
    # ALTER TABLE dollar_data ADD CONSTRAINT uq_dollar_date UNIQUE (date_miladi);

    upsert_sql = """
        INSERT INTO dollar_data (date_miladi, open, high, low, close)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date_miladi) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close= EXCLUDED.close;
    """
    cur.executemany(upsert_sql, list(df.itertuples(index=False, name=None)))
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {len(df)} ردیف درج/به‌روزرسانی شد.")

# -------------------- Main --------------------
if __name__ == "__main__":
    rows = scrape_all_pages()
    print(f"📦 جمع‌آوری: {len(rows)} ردیف (همهٔ صفحات).")
    df = to_dataframe(rows)
    upsert_to_db(df)
