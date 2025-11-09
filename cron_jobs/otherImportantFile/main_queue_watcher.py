# -*- coding: utf-8 -*-
import os, sys, time, logging, requests, psycopg2
from datetime import datetime
from dotenv import load_dotenv

# ============== تنظیمات از .env (با مقادیر پیش‌فرض) ==============
load_dotenv()
DB_URL = os.getenv("DB_URL_SYNC") or os.getenv("DB_URL")
if not DB_URL:
    print("❌ DB_URL/DB_URL_SYNC not set in .env", file=sys.stderr)
    sys.exit(1)

CHECK_INTERVAL_SEC   = int(os.getenv("QUEUE_CHECK_INTERVAL_SEC", "60"))   # هر چند ثانیه یکبار چک شود
MAX_WAIT_MINUTES     = int(os.getenv("QUEUE_MAX_WAIT_MINUTES",   "720"))  # حداکثر صبر (دقیقه)؛ پیش‌فرض 12 ساعت
REQUIRED_OK_COUNT    = int(os.getenv("QUEUE_REQUIRED_OK_COUNT",  "10"))   # چند نماد باید آپدیت شده باشند
MAX_TICKERS_TO_CHECK = int(os.getenv("QUEUE_MAX_TICKERS",        "300"))  # حداکثر چند نماد را در هر چرخه چک کنیم

LOG_PATH = os.getenv("QUEUE_LOG_PATH", "../../queue_fetch.log")
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

# ============== ابزار DB ==============
def pg_conn():
    return psycopg2.connect(DB_URL)

def get_last_trading_date_from_orderbook():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute('SELECT MAX("Timestamp"::date) FROM orderbook_snapshot;')
        dt = cur.fetchone()[0]
        if dt is None:
            raise RuntimeError("هیچ داده‌ای در orderbook_snapshot نیست.")
        return dt

def get_saham_inscodes(limit: int | None = None):
    """
    symboldetail باید ستون های: stock_ticker, "insCode" داشته باشد.
    توجه: در پروژه شما insCode با C بزرگ است.
    """
    sql = """
        SELECT DISTINCT "insCode"
        FROM symboldetail
        WHERE instrument_type = 'saham'
          AND "insCode" IS NOT NULL
        ORDER BY "insCode"
    """
    if limit:
        sql += f"\nLIMIT {int(limit)}"
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    # خروجی: لیست insCode به‌صورت رشته
    return [r[0] if isinstance(r[0], str) else str(r[0]) for r in rows]

# ============== چک منبع قدیمی TSETMC ==============
def get_latest_date_from_old_tsetmc(inscode: str):
    """
    منبع: old.tsetmc.com (InstTradeHistory)
    فقط آخرین رکورد را می‌خوانیم و تاریخ را برمی‌گردانیم (datetime.date)
    """
    url = f"http://old.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={inscode}&Top=1&A=0"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200 or not r.text.strip():
            return None
        # فرمت رکورد: 20251108@...
        yyyymmdd = r.text.split("@")[0].strip()
        dt = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        return dt
    except Exception as e:
        logging.warning(f"⚠️ fetch failed ({inscode}): {e}")
        return None

# ============== حلقهٔ واچر ==============
def main():
    try:
        target_date = get_last_trading_date_from_orderbook()
    except Exception as e:
        logging.error(f"❌ نتوانستم آخرین تاریخ orderbook_snapshot را بخوانم: {e}")
        sys.exit(1)

    logging.info(f"⏳ Watcher started. target_date={target_date}")

    # لیست کامل insCode های سهام
    try:
        all_inscodes = get_saham_inscodes(limit=None)  # همه را بگیر؛ با MAX_TICKERS_TO_CHECK کنترل می‌کنیم
        if not all_inscodes:
            logging.error("❌ هیچ insCode برای instrument_type='saham' پیدا نشد.")
            sys.exit(1)
    except Exception as e:
        logging.error(f"❌ خطا در خواندن symboldetail: {e}")
        sys.exit(1)

    tries = int((MAX_WAIT_MINUTES * 60) / CHECK_INTERVAL_SEC)

    for cycle in range(tries):
        ok_count = 0
        checked = 0

        # برای اینکه فشار نیاد، در هر چرخه فقط تا سقف MAX_TICKERS_TO_CHECK چک می‌کنیم
        for ins in all_inscodes[:MAX_TICKERS_TO_CHECK]:
            latest = get_latest_date_from_old_tsetmc(ins)
            checked += 1
            if latest and latest >= target_date:
                ok_count += 1
                if ok_count >= REQUIRED_OK_COUNT:
                    logging.info(f"✅ Source updated for at least {REQUIRED_OK_COUNT} symbols (checked={checked}). Exiting rc=0.")
                    sys.exit(0)

        logging.info(f"🔎 cycle={cycle+1} checked={checked} ok_count={ok_count} / required={REQUIRED_OK_COUNT} → منتظر می‌مانم {CHECK_INTERVAL_SEC}s")
        time.sleep(CHECK_INTERVAL_SEC)

    logging.warning("⌛ Timeout reached. Required count not satisfied. Exiting rc=2.")
    sys.exit(2)

if __name__ == "__main__":
    main()
