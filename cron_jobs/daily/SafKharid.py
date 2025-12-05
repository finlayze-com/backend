# -*- coding: utf-8 -*-
"""
محاسبه و ذخیره «فقط صف‌دارها» (صف خرید یا صف فروش) برای روز «دیروز معاملاتی»
- بدون استفاده از finpy_tse
- ذخیره در جدول قبلی (quote) با کلید (inscode, date)
- شامل Value روزانه از InstTradeHistory
- اضافه شدن base_value = adjust_high * baseVol
"""

import os
import logging
from datetime import datetime, timezone

import requests
import pandas as pd
import psycopg2
import jdatetime
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dotenv import load_dotenv

# ---------------------- تنظیمات عمومی ---------------------- #
HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 30  # ثانیه
CLOSE_HEVEN = 123000  # 12:30:00 → 123000 (HHMMSS)
CHUNK_SIZE = 1000

logging.basicConfig(
    filename='../../queue_fetch.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# --- کمکی‌های تاریخ --- #
def to_jalali_str(greg_date):
    """تبدیل تاریخ میلادیِ date به رشته جلالی YYYY-MM-DD"""
    return jdatetime.date.fromgregorian(date=greg_date).strftime('%Y-%m-%d')


def j2g_yyyymmdd(jdate_str: str) -> str:
    """تبدیل رشته جلالی YYYY-MM-DD به میلادی فشرده YYYYMMDD (برای API)"""
    y, m, d = map(int, jdate_str.split('-'))
    g = jdatetime.date(y, m, d).togregorian()
    return f"{g.year:04}{g.month:02}{g.day:02}"


def h_even_to_timestr(h: int) -> str:
    """تبدیل hEven مثل 123000 به HH:MM:SS"""
    s = str(int(h)).zfill(6)
    return f"{s[:2]}:{s[2:4]}:{s[4:]}"


# ---------------------- بارگذاری .env ---------------------- #
load_dotenv()
DB_URL_SYNC = os.getenv("DB_URL_SYNC") or os.getenv("DB_URL")
if not DB_URL_SYNC:
    raise EnvironmentError("❌ متغیر DB_URL یا DB_URL_SYNC در فایل .env یافت نشد.")

print("🔹 Start: connecting to DB ...")
conn = psycopg2.connect(DB_URL_SYNC)
cursor = conn.cursor()
logging.info("✅ اتصال به دیتابیس برقرار شد.")
print("   ✅ DB connected")

# ---------------------- تعیین تاریخ هدف ---------------------- #
try:
    print("🔹 Resolving target date from orderbook_snapshot ...")
    cursor.execute('SELECT MAX("Timestamp"::date) FROM orderbook_snapshot;')
    last_trading_date = cursor.fetchone()[0]
    if last_trading_date is None:
        raise RuntimeError("هیچ داده‌ای در جدول orderbook_snapshot وجود ندارد.")

    date_g = last_trading_date              # date میلادی
    date_j = to_jalali_str(date_g)          # جلالی YYYY-MM-DD
    date_g_compact = j2g_yyyymmdd(date_j)   # میلادی فشرده YYYYMMDD برای API

    msg = f"📅 Target Date → Gregorian={date_g} | Jalali={date_j} | Compact={date_g_compact}"
    logging.info(msg)
    print("   ✅", msg)
except Exception as e:
    logging.exception("❌ خطا در تعیین تاریخ هدف: %s", e)
    print("   ❌ Error determining target date:", e)
    cursor.close()
    conn.close()
    raise

# ---------------------- دریافت لیست نمادها و inscode ---------------------- #
try:
    print("🔹 Fetching saham tickers & insCodes from symboldetail ...")
    cursor.execute("""
        SELECT DISTINCT "stock_ticker", "insCode"
        FROM symboldetail
        WHERE instrument_type = 'saham'
          AND "stock_ticker" IS NOT NULL
          AND "insCode" IS NOT NULL
        ORDER BY "stock_ticker";
    """)
    tickers = [(r[0], str(r[1])) for r in cursor.fetchall()]
    logging.info(f"🔍 تعداد نمادهای سهام: {len(tickers)}")
    print(f"   ✅ Found {len(tickers)} saham tickers")
except Exception as e:
    logging.exception("❌ خطا در گرفتن لیست نمادها: %s", e)
    print("   ❌ Error fetching tickers:", e)
    cursor.close()
    conn.close()
    raise


# ---------------------- فراخوانی APIهای TSETMC ---------------------- #
def get_thresholds(inscode: str, yyyymmdd: str):
    """
    سقف/کف دامنه روز را برای تاریخ هدف می‌گیرد.
    خروجی: (day_ub, day_ll) به int
    """
    url = f"https://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{inscode}/{yyyymmdd}"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    js = r.json()
    df = pd.DataFrame(js.get("staticThreshold", []))
    if df.empty:
        raise RuntimeError("Empty staticThreshold")
    row = df.iloc[-1]
    day_ub = int(row["psGelStaMax"])
    day_ll = int(row["psGelStaMin"])
    return day_ub, day_ll


def get_bestlimits_snapshot(inscode: str, yyyymmdd: str):
    """
    از BestLimits آرشیوی همان روز، نزدیک‌ترین اسنپ‌شات به 12:30:00 را برمی‌گرداند.
    اگر هیچ اسنپ‌شاتی ≤ 12:30 نبود، آخرین اسنپ‌شات موجود همان روز را برمی‌گرداند.
    خروجی: dict یک ردیف (top level) یا None
    """
    url = f"https://cdn.tsetmc.com/api/BestLimits/{inscode}/{yyyymmdd}"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    js = r.json()

    # ساختار پاسخ ممکن است dict یا لیست باشد
    if isinstance(js, dict):
        rows = js.get("bestLimitsHistory", js.get("bestLimits", []))
    else:
        rows = js

    df = pd.DataFrame(rows)
    if df.empty:
        return None

    # hEven باید عددی باشد
    df = df[pd.to_numeric(df["hEven"], errors="coerce").notnull()]
    df["hEven"] = df["hEven"].astype(int)

    # اولویت: بزرگ‌ترین hEven ≤ 123000
    sub = df[df["hEven"] <= CLOSE_HEVEN]
    if not sub.empty:
        tmax = sub["hEven"].max()
        snap = sub[sub["hEven"] == tmax].sort_values("number").head(1).iloc[0].to_dict()
        return snap

    # در غیر این صورت، آخرین hEven روز
    tmax_all = df["hEven"].max()
    snap = df[df["hEven"] == tmax_all].sort_values("number").head(1).iloc[0].to_dict()
    return snap


def get_value_from_old_endpoint(inscode: str, yyyymmdd: str):
    """
    ارزش معاملات روزانه (Value) را از endpoint قدیمی می‌خواند و فقط همان تاریخ را برمی‌گرداند.
    اگر پیدا نشد → 0
    """
    url = f"https://old.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={inscode}&Top=999999&A=0"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    txt = r.text.strip()
    if not txt:
        return 0

    # columns=['Date','High','Low','Final','Close','Open','Y-Final','Value','Volume','No']
    last_value = 0
    for row in txt.split(";"):
        parts = row.split("@")
        if len(parts) < 9:
            continue
        d = parts[0]
        if d == yyyymmdd:
            try:
                last_value = int(float(parts[7]))
            except Exception:
                last_value = 0
            break
    return last_value


def compute_queues_from_snapshot(snap: dict, day_ub: int, day_ll: int):
    """
    محاسبه صف خرید/فروش و سرانه‌ها بر اساس snapshot خروجی BestLimits.
    - صف خرید: قیمت خرید سطح 1 == سقف روز
    - صف فروش: قیمت فروش سطح 1 == کف روز
    """
    p_buy = snap.get("pMeDem", snap.get("Price_Buy"))
    q_buy = snap.get("qTitMeDem", snap.get("Vol_Buy"))
    n_buy = snap.get("zOrdMeDem", snap.get("No_Buy"))

    p_sell = snap.get("pMeOf", snap.get("Price_Sell"))
    q_sell = snap.get("qTitMeOf", snap.get("Vol_Sell"))
    n_sell = snap.get("zOrdMeOf", snap.get("No_Sell"))

    p_buy = float(p_buy) if p_buy is not None else 0.0
    q_buy = int(q_buy) if q_buy is not None else 0
    n_buy = int(n_buy) if n_buy is not None else 0

    p_sell = float(p_sell) if p_sell is not None else 0.0
    q_sell = int(q_sell) if q_sell is not None else 0
    n_sell = int(n_sell) if n_sell is not None else 0

    bq_value = 0
    sq_value = 0
    bqpc = 0
    sqpc = 0

    # صف فروش
    if p_sell == float(day_ll):
        sq_value = int(day_ll * q_sell)
        sqpc = int(sq_value // max(n_sell, 1))

    # صف خرید
    if p_buy == float(day_ub):
        bq_value = int(day_ub * q_buy)
        bqpc = int(bq_value // max(n_buy, 1))

    time_close = h_even_to_timestr(int(snap.get("hEven", CLOSE_HEVEN)))
    return bq_value, sq_value, bqpc, sqpc, time_close


# ---------------------- دریافت adjust_high و baseVol برای همان روز ---------------------- #
def get_base_parts(inscode: str, yyyymmdd: str):
    """
    استخراج adjust_high و baseVol برای همان تاریخ:
      - adjust_high از InstTradeHistory (A=1 → تعدیل‌شده)
      - baseVol از GetInstrumentInfo (جدید)
    خروجی: (adjust_high, base_vol)
    """
    adjust_high = None
    base_vol = None

    # --- Adjusted High ---
    try:
        url = f"https://old.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={inscode}&Top=999999&A=1"
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        txt = r.text.strip()
        for row in txt.split(";"):
            parts = row.split("@")
            if len(parts) < 2:
                continue
            if parts[0] == yyyymmdd:
                adjust_high = float(parts[1])  # ستون High
                break
    except Exception as e:
        logging.warning(f"{inscode} - AdjustHigh fail: {e}")

    # --- BaseVol ---
    try:
        url = f"https://cdn.tsetmc.com/api/Instrument/GetInstrumentInfo/{inscode}"
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.ok:
            js = r.json()
            info = js.get("instrumentInfo", {})
            base_vol = int(float(info.get("baseVol", 0) or 0))
    except Exception as e:
        logging.warning(f"{inscode} - BaseVol fail: {e}")

    return adjust_high, base_vol


# ---------------------- پردازش فقط صف‌دارها ---------------------- #
engine = create_engine(DB_URL_SYNC)
records = []
downloaded_at = datetime.now(timezone.utc)

print(f"🔸 Processing {len(tickers)} tickers for date {date_g} (J:{date_j}) ...")

for idx, (stock_ticker, ins) in enumerate(tickers, start=1):
    if idx % 50 == 1 or idx == len(tickers):
        print(f"   … {idx}/{len(tickers)}")

    # لاگ اینکه الان کدام نماد در حال پردازش است
    print(f"→ Processing: {stock_ticker} ({ins})")
    logging.info(f"Processing: {stock_ticker} ({ins})")

    # --- مرحله ۱: Threshold ---
    try:
        day_ub, day_ll = get_thresholds(ins, date_g_compact)
    except Exception as e:
        logging.warning(f"{stock_ticker} ({ins}) - Threshold error: {e}")
        print(f"❌ Threshold error for {stock_ticker} ({ins}): {e}")
        continue

    # --- مرحله ۲: BestLimits snapshot ---
    try:
        snap = get_bestlimits_snapshot(ins, date_g_compact)
    except Exception as e:
        logging.warning(f"{stock_ticker} ({ins}) - BestLimits error: {e}")
        print(f"❌ BestLimits error for {stock_ticker} ({ins}): {e}")
        continue

    if not snap:
        logging.info(f"{stock_ticker} ({ins}) - No BestLimits snapshot, skipping.")
        print(f"⚠️ No BestLimits snapshot for {stock_ticker} ({ins}), skipping.")
        continue

    # --- مرحله ۳: محاسبه صف ---
    try:
        bq_value, sq_value, bqpc, sqpc, time_close = compute_queues_from_snapshot(
            snap, day_ub, day_ll
        )

        # دیباگ ویژه برای وسپه
        if stock_ticker == 'وسپه' or ins == '2328862017676109':
            print(f"   [DEBUG وسپه] day_ub={day_ub}, day_ll={day_ll}")
            print(
                f"   [DEBUG وسپه] bq_value={bq_value}, sq_value={sq_value}, "
                f"bqpc={bqpc}, sqpc={sqpc}, time_close={time_close}"
            )
            print(
                "   [DEBUG وسپه] snap fields: "
                f"pMeDem={snap.get('pMeDem')}, qTitMeDem={snap.get('qTitMeDem')}, "
                f"pMeOf={snap.get('pMeOf')}, qTitMeOf={snap.get('qTitMeOf')}, "
                f"hEven={snap.get('hEven')}"
            )
    except Exception as e:
        logging.warning(f"{stock_ticker} ({ins}) - Queue compute error: {e}")
        print(f"❌ Queue compute error for {stock_ticker} ({ins}): {e}")
        continue

    # فقط اگر صف خرید یا فروش باشد
    if bq_value <= 0 and sq_value <= 0:
        continue

    # --- مرحله ۴: سایر اطلاعات (Value, base_value) و ساخت رکورد ---
    try:
        day_value = get_value_from_old_endpoint(ins, date_g_compact)  # ارزش معاملات روز

        # گرفتن adjust_high و baseVol و محاسبه base_value
        adj_high, base_vol = get_base_parts(ins, date_g_compact)
        if adj_high is not None and base_vol is not None:
            try:
                base_value = float(adj_high) * int(base_vol)
            except Exception:
                base_value = 0
        else:
            base_value = 0

        rec = {
            # کلید یکتا
            "inscode": ins,
            "date": to_jalali_str(date_g),

            # سایر ستون‌ها
            "stock_ticker": stock_ticker,
            "downloaded_at": downloaded_at,
            "Day_UL": day_ub,
            "Day_LL": day_ll,
            "Time": time_close,
            "BQ_Value": bq_value,
            "SQ_Value": sq_value,
            "BQPC": bqpc,
            "SQPC": sqpc,
            "Value": day_value,       # ارزش معاملات روز
            "base_value": base_value  # مقدار جدید
        }
        records.append(rec)

    except Exception as e:
        logging.warning(f"{stock_ticker} ({ins}) - After-queue error: {e}")
        print(f"❌ After-queue error for {stock_ticker} ({ins}): {e}")
        continue

if not records:
    logging.warning("⚠️ هیچ رکورد صف‌داری برای ذخیره وجود ندارد.")
    print("\n🔻 No queue records to upsert.")
else:
    logging.info(f"📊 تعداد رکوردهای صف‌دار برای ذخیره: {len(records)}")
    print(f"\n🔹 Ready to UPSERT {len(records)} queued records into 'quote'")

    # --- UPSERT در جدول «quote» با همان کلید (inscode, date) --- #
    try:
        with engine.begin() as connection:
            md = MetaData()
            quote = Table("quote", md, autoload_with=connection)

            table_cols = {c.name for c in quote.columns}
            filtered_records = [
                {k: v for k, v in rec.items() if k in table_cols}
                for rec in records
            ]

            if not filtered_records:
                logging.warning("⚠️ بعد از فیلتر کردن ستون‌ها، چیزی برای نوشتن باقی نماند.")
                print("   ⚠️ Nothing left after column-filtering (check table schema).")
            else:
                insert_stmt = pg_insert(quote).values(filtered_records)

                conflict_cols = ["inscode", "date"]
                update_cols = [c for c in table_cols if c not in conflict_cols]

                do_update = insert_stmt.on_conflict_do_update(
                    index_elements=conflict_cols,
                    set_={c: getattr(insert_stmt.excluded, c) for c in update_cols}
                )
                connection.execute(do_update)
                print("   ✅ UPSERT executed.")
        print("✅ Done.")
    except Exception as e:
        print("   ❌ UPSERT error:", e)

cursor.close()
conn.close()
