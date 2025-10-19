# -*- coding: utf-8 -*-
"""
Build weekly haghighi (real/legal flows) from daily haghighi table using loader & writer.
- هماهنگ با ساختار واقعی دیتابیس کاربر (symbol, recdate, week_start/week_end)
- بدون تغییر نام ستون‌ها در دیتابیس
- محاسبات دلاری مطابق نسخه‌ی روزانه
"""

import os
import sys
import pandas as pd
from typing import Set, Tuple, Dict
from sqlalchemy import text

# --- import fix for both "module" and "direct" runs ---
try:
    from .loader import get_engine, load_table, get_last_week_end
    from .writer import upsert_dataframe
except ImportError:
    THIS_DIR = os.path.dirname(__file__)
    if THIS_DIR not in sys.path:
        sys.path.append(THIS_DIR)
    from loader import get_engine, load_table, get_last_week_end
    from writer import upsert_dataframe
# --- end import fix ---


def _get_table_columns(engine, table_name: str) -> Set[str]:
    """دریافت نام ستون‌های جدول مقصد از information_schema."""
    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :tname
        ORDER BY ordinal_position
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"tname": table_name}).fetchall()
    return {r[0] for r in rows}


def build_weekly_haghighi_from_daily(
    src_table: str = "haghighi",
    dst_table: str = "weekly_haghighi",

    date_col: str = "recdate",
    symbol_col: str = "symbol",
    dest_symbol_col: str = "symbol",

    # ستون‌های حقیقی/حقوقی
    buy_i_volume_col: str = "buy_i_volume",
    buy_i_value_col: str = "buy_i_value",
    sell_i_volume_col: str = "sell_i_volume",
    sell_i_value_col: str = "sell_i_value",
    buy_n_volume_col: str = "buy_n_volume",
    buy_n_value_col: str = "buy_n_value",
    sell_n_volume_col: str = "sell_n_volume",
    sell_n_value_col: str = "sell_n_value",

    # شمارنده‌ها
    buy_n_count_col: str = "buy_n_count",
    buy_i_count_col: str = "buy_i_count",
    sell_n_count_col: str = "sell_n_count",
    sell_i_count_col: str = "sell_i_count",

    # ستون‌های دلاری
    buy_i_value_usd_col: str = "buy_i_value_usd",
    buy_n_value_usd_col: str = "buy_n_value_usd",
    sell_i_value_usd_col: str = "sell_i_value_usd",
    sell_n_value_usd_col: str = "sell_n_value_usd",

    conflict_on: Tuple[str, str] = ("symbol", "week_end"),
):
    """ایجاد جدول هفتگی از داده‌های روزانه haghighi بدون تغییر اسکیمای DB."""
    eng = get_engine()
    print(f"🔄 Building weekly haghighi: {src_table} → {dst_table}")

    # ستون‌های موجود در مقصد
    dst_cols: Set[str] = _get_table_columns(eng, dst_table)

    # آخرین هفته ثبت‌شده
    last_week_end = get_last_week_end(eng, dst_table)

    # داده‌های روزانه
    df = load_table(eng, src_table)
    if df.empty:
        print("⚠️ Daily haghighi source is empty.")
        return

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col, symbol_col]).sort_values([symbol_col, date_col])
    src_exist = set(df.columns)

    # فقط ستون‌هایی که واقعاً وجود دارند
    sum_candidates: Dict[str, str] = {}
    for c in [
        buy_i_volume_col, buy_i_value_col, sell_i_volume_col, sell_i_value_col,
        buy_n_volume_col, buy_n_value_col, sell_n_volume_col, sell_n_value_col,
        buy_n_count_col, buy_i_count_col, sell_n_count_col, sell_i_count_col,
        buy_i_value_usd_col, buy_n_value_usd_col, sell_i_value_usd_col, sell_n_value_usd_col,
    ]:
        if c in src_exist:
            sum_candidates[c] = "sum"

    all_weekly = []

    for sym, g in df.groupby(symbol_col, sort=False):
        if g.empty:
            continue

        gd = g.set_index(date_col)
        wk = gd.groupby(pd.Grouper(freq="W-FRI", label="left", closed="left")).agg(sum_candidates)
        wk = wk.reset_index().rename(columns={date_col: "week_start"})
        if wk.empty:
            continue

        wk["week_end"] = wk["week_start"] + pd.Timedelta(days=6)
        #wk = wk[wk["week_end"] > last_week_end]
        if wk.empty:
            continue

        wk[dest_symbol_col] = sym

        # inflow/outflow ریالی
        if buy_i_value_col in wk.columns and sell_i_value_col in wk.columns:
            wk["haghighi_inflow"] = wk[buy_i_value_col] - wk[sell_i_value_col]
            wk["haghighi_outflow"] = wk[sell_i_value_col] - wk[buy_i_value_col]

        # inflow/outflow دلاری
        if buy_i_value_usd_col in wk.columns and sell_i_value_usd_col in wk.columns:
            if "haghighi_inflow_usd" in dst_cols:
                wk["haghighi_inflow_usd"] = wk[buy_i_value_usd_col] - wk[sell_i_value_usd_col]
            if "haghighi_outflow_usd" in dst_cols:
                wk["haghighi_outflow_usd"] = wk[sell_i_value_usd_col] - wk[buy_i_value_usd_col]

        base_key_cols = [dest_symbol_col, "week_start", "week_end"]

        preferred_order = [
            buy_i_volume_col, buy_i_value_col, sell_i_volume_col, sell_i_value_col,
            buy_n_volume_col, buy_n_value_col, sell_n_volume_col, sell_n_value_col,
            buy_n_count_col, buy_i_count_col, sell_n_count_col, sell_i_count_col,
            buy_i_value_usd_col, buy_n_value_usd_col, sell_i_value_usd_col, sell_n_value_usd_col,
            "haghighi_inflow", "haghighi_outflow",
            "haghighi_inflow_usd", "haghighi_outflow_usd",
        ]

        cols_existing_in_wk = [c for c in preferred_order if c in wk.columns]
        cols_existing_in_dst = [c for c in cols_existing_in_wk if c in dst_cols]
        final_cols = base_key_cols + cols_existing_in_dst

        wk = wk[final_cols]
        all_weekly.append(wk)

    if not all_weekly:
        print("📭 No new weekly haghighi rows to insert.")
        return

    out = pd.concat(all_weekly, ignore_index=True)
    out["week_start"] = pd.to_datetime(out["week_start"]).dt.date
    out["week_end"] = pd.to_datetime(out["week_end"]).dt.date

    # درج نهایی
    upsert_dataframe(out, eng, dst_table, conflict_cols=conflict_on)
    print(f"✅ {len(out)} rows upserted into '{dst_table}'.")


# --- direct run ---
if __name__ == "__main__":
    print("🔄 Running Weekly Haghighi Builder (direct)")
    build_weekly_haghighi_from_daily(
        src_table="haghighi",
        dst_table="weekly_haghighi",
        date_col="recdate",
        symbol_col="symbol",
        dest_symbol_col="symbol",      # 👈 مطابق ساختار واقعی جدول
        conflict_on=("symbol", "week_end"),  # 👈 کلید اصلی واقعی
    )
