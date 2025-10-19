# -*- coding: utf-8 -*-
"""
هسته‌ی محاسبه اندیکاتورهای هفتگی (نسخه ماژولار با loader و writer).
- ورودی: جدول هفتگی منبع (مثل weekly_stock_data)
- خروجی: درج در جدول اندیکاتور هفتگی (مثل weekly_indicators)
- شامل اندیکاتورهای EMA، RSI، MACD، Ichimoku، ATR، Renko (نسخه ریالی و دلاری)
- از loader برای اتصال و خواندن استفاده می‌کند و از writer برای UPSERT (در این نسخه، UPSERT دستی با psycopg2)
"""

from __future__ import annotations

import math
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from .loader import get_engine, load_table
from .writer import upsert_dataframe  # اگر جای دیگری خواستی استفاده کنی، اینجا ایمپورت شده

# ---------------------------------------------------------------
# بررسی TA-Lib
# ---------------------------------------------------------------
try:
    import talib
    HAS_TALIB = True
except Exception:
    HAS_TALIB = False


# ==============================================================
# 📊 اندیکاتورهای پایه (با fallback در صورت نبود TA-Lib)
# ==============================================================

def _ema(series: pd.Series, period: int) -> pd.Series:
    if HAS_TALIB:
        return pd.Series(
            talib.EMA(series.values.astype(float), timeperiod=period),
            index=series.index
        )
    return series.ewm(span=period, adjust=False).mean()


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    if HAS_TALIB:
        return pd.Series(
            talib.RSI(series.values.astype(float), timeperiod=period),
            index=series.index
        )
    # fallback manual RSI
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _macd(series: pd.Series, fast=12, slow=26, signal=9):
    if HAS_TALIB:
        macd, macd_sig, macd_hist = talib.MACD(
            series.values.astype(float),
            fastperiod=fast,
            slowperiod=slow,
            signalperiod=signal,
        )
        idx = series.index
        return pd.Series(macd, idx), pd.Series(macd_sig, idx), pd.Series(macd_hist, idx)
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_sig = macd.ewm(span=signal, adjust=False).mean()
    return macd, macd_sig, macd - macd_sig


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 22) -> pd.Series:
    if HAS_TALIB:
        out = talib.ATR(
            high.values.astype(float),
            low.values.astype(float),
            close.values.astype(float),
            timeperiod=period,
        )
        return pd.Series(out, index=close.index)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(window=period).mean()


def _ichimoku(high: pd.Series, low: pd.Series, close: pd.Series):
    # استاندارد: 9, 26, 52
    tenkan = (high.rolling(9).max() + low.rolling(9).min()) / 2
    kijun = (high.rolling(26).max() + low.rolling(26).min()) / 2
    senkou_a = ((tenkan + kijun) / 2).shift(26)
    senkou_b = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    chikou = close.shift(-26)
    return tenkan, kijun, senkou_a, senkou_b, chikou


def _renko_direction(close: pd.Series, box_size: float) -> pd.Series:
    """
    رنکوی ساده: تغییر جهت با عبور از گام مشخص box_size
    خروجی: سری از {'UP','DOWN', None}
    """
    if box_size is None or (isinstance(box_size, float) and (math.isnan(box_size) or box_size <= 0)):
        return pd.Series([None] * len(close), index=close.index, dtype=object)
    if close.empty:
        return pd.Series([], dtype=object)

    direction = []
    last_brick = close.iloc[0]
    curr_dir = None
    for c in close:
        if curr_dir in (None, "UP"):
            while c >= last_brick + box_size:
                curr_dir = "UP"
                last_brick += box_size
            while c <= last_brick - box_size:
                curr_dir = "DOWN"
                last_brick -= box_size
        else:  # DOWN
            while c <= last_brick - box_size:
                curr_dir = "DOWN"
                last_brick -= box_size
            while c >= last_brick + box_size:
                curr_dir = "UP"
                last_brick += box_size
        direction.append(curr_dir)
    return pd.Series(direction, index=close.index, dtype=object)


# ==============================================================
# 🧰 Utility
# ==============================================================

def _py(v):
    """
    تبدیل انواع numpy به انواع Python native برای psycopg2
    """
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            return v
    return v


# ==============================================================
# ⚙️ هسته‌ی محاسبه اندیکاتورهای هفتگی
# ==============================================================

def build_weekly_indicators_for_table(source_table: str, dest_table: str, insert_mode: str = "upsert"):
    """
    از جدول هفتگی منبع می‌خواند، برای هر نماد اندیکاتورهای ریالی و دلاری را محاسبه می‌کند،
    سپس در جدول اندیکاتور مقصد UPSERT یا REPLACE می‌نماید.

    Parameters
    ----------
    source_table : str
        نام جدول هفتگی منبع (مثلاً 'weekly_stock_data')
    dest_table : str
        نام جدول مقصد برای اندیکاتورها (مثلاً 'weekly_indicators')
    insert_mode : {'upsert','replace_all'}
        - upsert: درگیری با ON CONFLICT
        - replace_all: قبل از درج، رکوردهای نماد حذف می‌شوند
    """
    print(f"🔄 شروع محاسبه اندیکاتورهای هفتگی برای جدول: {source_table}")

    engine = get_engine()
    df_all = load_table(engine, source_table)

    if df_all.empty:
        print(f"⚠️ جدول {source_table} خالی است.")
        return

    # ✅ اتصال درست به دیتابیس بدون ماسک‌شدن پسورد
    # ✅ اتصال DBAPI بدون context manager (و بستن امن در finally)
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        try:
            # ✅ خواندن ستون‌های واقعی جدول مقصد
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s
            """, (dest_table,))
            dest_cols = {row[0] for row in cur.fetchall()}
            if not dest_cols:
                raise RuntimeError(f"❌ جدول مقصد '{dest_table}' یافت نشد یا ستونی ندارد.")
            # ✅ اطمینان از وجود کلیدها در جدول مقصد
            if "stock_ticker" not in dest_cols or "week_end" not in dest_cols:
                raise RuntimeError("❌ جدول مقصد باید ستون‌های 'stock_ticker' و 'week_end' را داشته باشد.")

            # نمادها
            if "stock_ticker" not in df_all.columns:
                raise RuntimeError(f"❌ ستون 'stock_ticker' در {source_table} وجود ندارد.")
            if "week_end" not in df_all.columns:
                raise RuntimeError(f"❌ ستون 'week_end' در {source_table} وجود ندارد.")

            tickers = df_all["stock_ticker"].dropna().unique().tolist()
            if not tickers:
                print(f"⚠️ هیچ نمادی در {source_table} یافت نشد.")
                return

            total_rows = 0

            for i, t in enumerate(tickers, 1):
                print(f"[{i}/{len(tickers)}] ⏳ در حال پردازش نماد: {t}")

                df = df_all[df_all["stock_ticker"] == t].sort_values("week_end").reset_index(drop=True)
                if df.empty:
                    continue

                # =============================
                # بخش ریالی
                # =============================
                close = pd.to_numeric(df.get("adjust_close"), errors="coerce")
                high  = pd.to_numeric(df.get("adjust_high"),  errors="coerce")
                low   = pd.to_numeric(df.get("adjust_low"),   errors="coerce")

                ema20   = _ema(close, 20)
                ema50   = _ema(close, 50)
                ema100  = _ema(close, 100)
                rsi14   = _rsi(close, 14)
                macd, macd_sig, macd_hist = _macd(close)
                tenkan, kijun, senkou_a, senkou_b, chikou = _ichimoku(high, low, close)
                atr22   = _atr(high, low, close, 22)
                last_box = atr22.dropna().iloc[-1] if not atr22.dropna().empty else np.nan
                renko_dir = _renko_direction(close, float(last_box) if pd.notna(last_box) else None)

                sig_ichimoku_buy   = ((tenkan > kijun) & (tenkan.shift(1) < kijun.shift(1))).astype("Int64").fillna(0)
                sig_ichimoku_sell  = ((tenkan < kijun) & (tenkan.shift(1) > kijun.shift(1))).astype("Int64").fillna(0)
                sig_ema_cross_buy  = ((ema20.shift(1) < ema50.shift(1)) & (ema20 > ema50)).astype("Int64").fillna(0)
                sig_ema_cross_sell = ((ema20.shift(1) > ema50.shift(1)) & (ema20 < ema50)).astype("Int64").fillna(0)
                sig_rsi_buy        = ((rsi14.shift(1) < 30) & (rsi14 > 30)).astype("Int64").fillna(0)
                sig_rsi_sell       = ((rsi14.shift(1) > 70) & (rsi14 < 70)).astype("Int64").fillna(0)
                sig_macd_buy       = ((macd.shift(1) < macd_sig.shift(1)) & (macd > macd_sig)).astype("Int64").fillna(0)
                sig_macd_sell      = ((macd.shift(1) > macd_sig.shift(1)) & (macd < macd_sig)).astype("Int64").fillna(0)
                sig_ema50_100_buy  = ((ema50.shift(1) < ema100.shift(1)) & (ema50 > ema100) & (ema20 > ema100)).astype("Int64").fillna(0)
                sig_ema50_100_sell = ((ema50.shift(1) > ema100.shift(1)) & (ema50 < ema100) & (ema20 < ema100)).astype("Int64").fillna(0)

                # =============================
                # بخش دلاری
                # =============================
                close_d = pd.to_numeric(df.get("adjust_close_usd"), errors="coerce")
                high_d  = pd.to_numeric(df.get("adjust_high_usd"),  errors="coerce")
                low_d   = pd.to_numeric(df.get("adjust_low_usd"),   errors="coerce")

                ema20_d   = _ema(close_d, 20)
                ema50_d   = _ema(close_d, 50)
                ema100_d  = _ema(close_d, 100)
                rsi14_d   = _rsi(close_d, 14)
                macd_d, macd_sig_d, macd_hist_d = _macd(close_d)
                tenkan_d, kijun_d, senkou_a_d, senkou_b_d, chikou_d = _ichimoku(high_d, low_d, close_d)
                atr22_d   = _atr(high_d, low_d, close_d, 22)
                last_box_d = atr22_d.dropna().iloc[-1] if not atr22_d.dropna().empty else np.nan
                renko_dir_d = _renko_direction(close_d, float(last_box_d) if pd.notna(last_box_d) else None)

                sig_ichimoku_buy_d   = ((tenkan_d > kijun_d) & (tenkan_d.shift(1) < kijun_d.shift(1))).astype("Int64").fillna(0)
                sig_ichimoku_sell_d  = ((tenkan_d < kijun_d) & (tenkan_d.shift(1) > kijun_d.shift(1))).astype("Int64").fillna(0)
                sig_ema_cross_buy_d  = ((ema20_d.shift(1) < ema50_d.shift(1)) & (ema20_d > ema50_d)).astype("Int64").fillna(0)
                sig_ema_cross_sell_d = ((ema20_d.shift(1) > ema50_d.shift(1)) & (ema20_d < ema50_d)).astype("Int64").fillna(0)
                sig_rsi_buy_d        = ((rsi14_d.shift(1) < 30) & (rsi14_d > 30)).astype("Int64").fillna(0)
                sig_rsi_sell_d       = ((rsi14_d.shift(1) > 70) & (rsi14_d < 70)).astype("Int64").fillna(0)
                sig_macd_buy_d       = ((macd_d.shift(1) < macd_sig_d.shift(1)) & (macd_d > macd_sig_d)).astype("Int64").fillna(0)
                sig_macd_sell_d      = ((macd_d.shift(1) > macd_sig_d.shift(1)) & (macd_d < macd_sig_d)).astype("Int64").fillna(0)
                sig_ema50_100_buy_d  = ((ema50_d.shift(1) < ema100_d.shift(1)) & (ema50_d > ema100_d) & (ema20_d > ema100_d)).astype("Int64").fillna(0)
                sig_ema50_100_sell_d = ((ema50_d.shift(1) > ema100_d.shift(1)) & (ema50_d < ema100_d) & (ema20_d < ema100_d)).astype("Int64").fillna(0)

                # =============================
                # ساخت DataFrame خروجی
                # =============================
                block = pd.DataFrame({
                    "stock_ticker": df["stock_ticker"].values,
                    "week_end": df["week_end"].values,

                    # --- ریالی ---
                    "ema_20": ema20, "ema_50": ema50, "ema_100": ema100,
                    "rsi": rsi14,
                    "macd": macd, "macd_signal": macd_sig, "macd_hist": macd_hist,
                    "tenkan": tenkan, "kijun": kijun, "senkou_a": senkou_a, "senkou_b": senkou_b, "chikou": chikou,
                    "signal_ichimoku_buy": sig_ichimoku_buy, "signal_ichimoku_sell": sig_ichimoku_sell,
                    "signal_ema_cross_buy": sig_ema_cross_buy, "signal_ema_cross_sell": sig_ema_cross_sell,
                    "signal_rsi_buy": sig_rsi_buy, "signal_rsi_sell": sig_rsi_sell,
                    "signal_macd_buy": sig_macd_buy, "signal_macd_sell": sig_macd_sell,
                    "signal_ema50_100_buy": sig_ema50_100_buy, "signal_ema50_100_sell": sig_ema50_100_sell,
                    "atr_22": atr22, "renko_22": renko_dir,

                    # --- دلاری ---
                    "ema_20_d": ema20_d, "ema_50_d": ema50_d, "ema_100_d": ema100_d,
                    "rsi_d": rsi14_d,
                    "macd_d": macd_d, "macd_signal_d": macd_sig_d, "macd_hist_d": macd_hist_d,
                    "tenkan_d": tenkan_d, "kijun_d": kijun_d, "senkou_a_d": senkou_a_d, "senkou_b_d": senkou_b_d, "chikou_d": chikou_d,
                    "signal_ichimoku_buy_d": sig_ichimoku_buy_d, "signal_ichimoku_sell_d": sig_ichimoku_sell_d,
                    "signal_ema_cross_buy_d": sig_ema_cross_buy_d, "signal_ema_cross_sell_d": sig_ema_cross_sell_d,
                    "signal_rsi_buy_d": sig_rsi_buy_d, "signal_rsi_sell_d": sig_rsi_sell_d,
                    "signal_macd_buy_d": sig_macd_buy_d, "signal_macd_sell_d": sig_macd_sell_d,
                    "signal_ema50_100_buy_d": sig_ema50_100_buy_d, "signal_ema50_100_sell_d": sig_ema50_100_sell_d,
                    "atr_22_d": atr22_d, "renko_22_d": renko_dir_d,
                })

                # سلامت کلیدها
                block = block.dropna(subset=["week_end", "stock_ticker"])
                block["week_end"] = pd.to_datetime(block["week_end"]).dt.date

                # تبدیل انواع numpy → Python-native
                for c in block.columns:
                    block[c] = block[c].apply(_py)

                # ✅ فقط ستون‌هایی که در جدول مقصد وجود دارند را نگه داریم (عدم تغییر اسکیمای DB)
                keep_cols = [c for c in block.columns if c in dest_cols]
                block = block[keep_cols]

                # ✅ اطمینان از وجود کلیدها بعد از فیلتر
                if not {"stock_ticker", "week_end"}.issubset(block.columns):
                    print(f"⚠️ رد نماد {t}: کلیدهای لازم در block یافت نشد (پس از فیلتر ستون‌ها).")
                    continue

                if block.empty:
                    continue

                # درج در دیتابیس (UPSERT / REPLACE)
                if insert_mode == "replace_all":
                    cur.execute(f"DELETE FROM {dest_table} WHERE stock_ticker = %s", (t,))
                    execute_values(
                        cur,
                        f"INSERT INTO {dest_table} ({','.join(block.columns)}) VALUES %s",
                        block.to_records(index=False).tolist(),
                        page_size=1000,
                    )
                else:
                    cols = ",".join(block.columns)
                    update_str = ", ".join(
                        [f"{c}=EXCLUDED.{c}" for c in block.columns if c not in ("stock_ticker", "week_end")]
                    )
                    sql = f"""
                        INSERT INTO {dest_table} ({cols}) VALUES %s
                        ON CONFLICT (stock_ticker, week_end) DO UPDATE SET {update_str}
                    """
                    execute_values(cur, sql, block.to_records(index=False).tolist(), page_size=1000)

                total_rows += len(block)

            conn.commit()
            print(f"✅ {total_rows} ردیف اندیکاتور در {dest_table} درج یا به‌روزرسانی شد.")
        finally:
            cur.close()
    finally:
        conn.close()


# ==============================================================
# نمونه استفاده (اختیاری)
# ==============================================================
if __name__ == "__main__":
    # مثال: محاسبه اندیکاتور هفتگی برای سهام
    build_weekly_indicators_for_table(
        source_table="weekly_stock_data",
        dest_table="weekly_indicators",
        insert_mode="upsert",  # یا "replace_all"
    )
