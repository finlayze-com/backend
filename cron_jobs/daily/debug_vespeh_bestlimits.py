# debug_vespeh_bestlimits.py
# -*- coding: utf-8 -*-

import json
from datetime import date
import requests
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

INSCODE = "2328862017676109"   # وسپه
YMD = "20251203"               # تاریخ میلادی فشرده
CLOSE_HEVEN = 123000           # همان چیزی که در SafKharid داری (hEven <= 12:30)

# اگر در SafKharid برای HEADERS چیز خاصی گذاشتی، همین را کپی کن
HEADERS = {
    "User-Agent": "Mozilla/5.0",
}

def debug_thresholds():
    url = f"https://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{INSCODE}/{YMD}"
    r = requests.get(url, headers=HEADERS, timeout=10, verify=False)
    print("🔹 Threshold status:", r.status_code)
    print("🔹 Raw Threshold JSON:", r.text)
    js = r.json()
    rows = js.get("staticThreshold", js)
    print("🔹 Parsed staticThreshold rows:")
    print(json.dumps(rows, indent=2, ensure_ascii=False))

    # پیدا کردن رکورد مربوط به خود 20251203
    day_ub = None
    day_ll = None
    for row in rows:
        if str(row.get("dEven")) == YMD:
            day_ub = float(row.get("psGelStaMax", 0) or 0)
            day_ll = float(row.get("psGelStaMin", 0) or 0)
    print(f"\n✅ day_ub = {day_ub} | day_ll = {day_ll}\n")
    return day_ub, day_ll


def debug_bestlimits(day_ub, day_ll):
    url = f"https://cdn.tsetmc.com/api/BestLimits/{INSCODE}/{YMD}"
    print("🔹 BestLimits URL:", url)
    r = requests.get(url, headers=HEADERS, timeout=10, verify=False)
    print("🔹 BestLimits status:", r.status_code)

    try:
        js = r.json()
    except Exception as e:
        print("❌ Error parsing JSON:", e)
        print("Raw text:", r.text[:500])
        return

    # ساخت DataFrame شبیه SafKharid
    if isinstance(js, dict):
        rows = js.get("bestLimitsHistory", js.get("bestLimits", []))
    else:
        rows = js

    df = pd.DataFrame(rows)
    print("\n🔹 Columns in BestLimits DF:")
    print(df.columns.tolist())

    if df.empty:
        print("❌ BestLimits dataframe is EMPTY")
        return

    # hEven را به عدد تبدیل کنیم
    df = df.copy()
    df["hEven_num"] = pd.to_numeric(df["hEven"], errors="coerce")
    df = df[df["hEven_num"].notnull()]
    df["hEven_num"] = df["hEven_num"].astype(int)

    print("\n🔹 Head of DF (first 10 rows):")
    print(df.head(10))

    print("\n🔹 hEven_num min/max:", df["hEven_num"].min(), df["hEven_num"].max())

    # همان منطق SafKharid: اول hEven <= 123000
    sub = df[df["hEven_num"] <= CLOSE_HEVEN]

    if not sub.empty:
        tmax = sub["hEven_num"].max()
        chosen = sub[sub["hEven_num"] == tmax].sort_values("number").head(1)
        source = f"≤ {CLOSE_HEVEN}"
    else:
        tmax = df["hEven_num"].max()
        chosen = df[df["hEven_num"] == tmax].sort_values("number").head(1)
        source = "ALL (fallback)"

    print(f"\n🔹 Chosen snapshot (source = {source}, hEven_num = {tmax}):")
    print(chosen.T)

    row = chosen.iloc[0]

    # استخراج فیلدهای خرید/فروش، با پوشش نام‌های مختلف
    p_buy = float(row.get("pMeDem", row.get("Price_Buy", 0)) or 0)
    q_buy = int(row.get("qTitMeDem", row.get("Vol_Buy", 0)) or 0)
    n_buy = int(row.get("zOrdMeDem", row.get("No_Buy", 0)) or 0)

    p_sell = float(row.get("pMeOf", row.get("Price_Sell", 0)) or 0)
    q_sell = int(row.get("qTitMeOf", row.get("Vol_Sell", 0)) or 0)
    n_sell = int(row.get("zOrdMeOf", row.get("No_Sell", 0)) or 0)

    print("\n🔹 Parsed buy/sell from chosen row:")
    print(f"   p_buy  = {p_buy}")
    print(f"   q_buy  = {q_buy}")
    print(f"   n_buy  = {n_buy}")
    print(f"   p_sell = {p_sell}")
    print(f"   q_sell = {q_sell}")
    print(f"   n_sell = {n_sell}")

    # شبیه compute_queues_from_snapshot
    bq_value = 0
    sq_value = 0
    bqpc = 0
    sqpc = 0

    if p_sell == float(day_ll):
        sq_value = int(day_ll * q_sell)
        sqpc = int(sq_value // max(n_sell, 1))

    if p_buy == float(day_ub):
        bq_value = int(day_ub * q_buy)
        bqpc = int(bq_value // max(n_buy, 1))

    print("\n🔹 Result of queue detection (like compute_queues_from_snapshot):")
    print(f"   day_ub = {day_ub}, day_ll = {day_ll}")
    print(f"   bq_value = {bq_value}, bqpc = {bqpc}")
    print(f"   sq_value = {sq_value}, sqpc = {sqpc}")

    if bq_value > 0:
        print("\n✅ From API point of view: BUY QUEUE detected.")
    elif sq_value > 0:
        print("\n✅ From API point of view: SELL QUEUE detected.")
    else:
        print("\n⚠️ From API point of view: NO QUEUE detected with current logic.")


if __name__ == "__main__":
    print("====== DEBUG VESPEH (2328862017676109) – 2025-12-03 ======\n")
    day_ub, day_ll = debug_thresholds()
    if day_ub is None:
        print("❌ Could not find thresholds for this date.")
    else:
        debug_bestlimits(day_ub, day_ll)
