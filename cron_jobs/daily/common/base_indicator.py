# cron_jobs/daily/common/base_indicator.py
# -*- coding: utf-8 -*-
"""
هسته‌ی محاسبه اندیکاتورها برای هر جدول روزانه.
- ورودی: نام جدول سورس (مثلاً: daily_stock_data, daily_fund_gold, ...)
- خروجی: درج در جدول اندیکاتور مقصد (مثلاً: daily_indicators, daily_indicators_fund_gold, ...)
- منطق کاملاً هم‌راستای اسکریپت قدیمی update_daily_indicator_for_All_Data.py است.
- از TA-Lib استفاده می‌کند؛ اگر نصب نبود با fallback محاسبه می‌شود.

نکته:
- از همان متغیر محیطی DB_URL_SYNC استفاده می‌کنیم (مثل base_updater).
- درج به صورت upsert ساده یا پاک‌سازی و درج مجدد؛ با فلگ INSERT_MODE قابل تنظیم است.
"""

from __future__ import annotations
import os
import math
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from sqlalchemy import create_engine

# تلاش برای استفاده از TA-Lib؛ اگر نبود fallback بکار می‌افتد
try:
    import talib
    HAS_TALIB = True
except Exception:
    HAS_TALIB = False


# ---------------------------
# اندیکاتورها: با TA-Lib یا fallback
# ---------------------------

def _ema(series: pd.Series, period: int) -> pd.Series:
    if HAS_TALIB:
        return pd.Series(talib.EMA(series.values.astype(float), timeperiod=period), index=series.index)
    return series.ewm(span=period, adjust=False).mean()

def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    if HAS_TALIB:
        return pd.Series(talib.RSI(series.values.astype(float), timeperiod=period), index=series.index)
    # fallback RSI
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def _macd(series: pd.Series, fast=12, slow=26, signal=9):
    if HAS_TALIB:
        macd, macd_signal, macd_hist = talib.MACD(series.values.astype(float), fastperiod=fast, slowperiod=slow, signalperiod=signal)
        idx = series.index
        return pd.Series(macd, index=idx), pd.Series(macd_signal, index=idx), pd.Series(macd_hist, index=idx)
    # fallback MACD با EMA
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=signal, adjust=False).mean()
    macd_hist = macd - macd_signal
    return macd, macd_signal, macd_hist

def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 22) -> pd.Series:
    if HAS_TALIB:
        out = talib.ATR(high.values.astype(float), low.values.astype(float), close.values.astype(float), timeperiod=period)
        return pd.Series(out, index=close.index)
    # fallback ATR
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr

def _ichimoku(high: pd.Series, low: pd.Series, close: pd.Series):
    # پارامترهای استاندارد: 9, 26, 52
    tenkan = (high.rolling(9).max() + low.rolling(9).min()) / 2
    kijun = (high.rolling(26).max() + low.rolling(26).min()) / 2
    senkou_a = ((tenkan + kijun) / 2).shift(26)
    senkou_b = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    chikou = close.shift(-26)
    return tenkan, kijun, senkou_a, senkou_b, chikou


# ----------------------------------
# رنکو: جهت حرکت با جعبه ATR22
# خروجی: "UP" / "DOWN" / None
# ----------------------------------
def _renko_direction(close: pd.Series, box_size: float) -> pd.Series:
    """
    ساده: هرگاه close نسبت به آخرین آجر از box_size عبور کند، جهت تغییر می‌کند.
    نتیجه به صورت جهت جاری است (UP/DOWN).
    """
    if box_size is None or (isinstance(box_size, float) and (math.isnan(box_size) or box_size <= 0)):
        return pd.Series([None] * len(close), index=close.index)

    direction = []
    if close.empty:
        return pd.Series([], dtype=object)
    last_brick = close.iloc[0]
    curr_dir = None  # "UP" یا "DOWN"
    for c in close:
        if curr_dir in (None, "UP"):
            while c >= last_brick + box_size:
                curr_dir = "UP"
                last_brick = last_brick + box_size
            while c <= last_brick - box_size:
                curr_dir = "DOWN"
                last_brick = last_brick - box_size
        else:  # curr_dir == "DOWN"
            while c <= last_brick - box_size:
                curr_dir = "DOWN"
                last_brick = last_brick - box_size
            while c >= last_brick + box_size:
                curr_dir = "UP"
                last_brick = last_brick + box_size
        direction.append(curr_dir)
    return pd.Series(direction, index=close.index, dtype=object)


# ---------------------------
# Utility
# ---------------------------

def _resolve_db_url() -> str:
    """
    تلاش برای بارگذاری .env از چند مسیر رایج (همان الگوی پروژه تو).
    """
    here = os.path.dirname(__file__)
    candidates = [
        os.path.join(here, "../../../.env"),  # cron_jobs/daily/common/../../../.env
        os.path.join(here, "../../../../.env"),
        os.path.join(here, ".env"),
    ]
    for p in candidates:
        if os.path.exists(p):
            load_dotenv(p)
            break
    db_url = os.getenv("DB_URL_SYNC")
    if not db_url:
        raise RuntimeError("DB_URL_SYNC not set in .env")
    return db_url


def _py(v):
    """
    تبدیل NaN/numpy types به Python-native برای psycopg2
    """
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    # numpy scalar -> python
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            return v
    return v


# ---------------------------
# هسته‌ی اجرا
# ---------------------------

def build_indicators_for_table(source_table: str, dest_table: str, insert_mode: str = "upsert"):
    """
    از جدول روزانه‌ی منبع می‌خواند، برای هر نماد مرتب بر اساس تاریخ محاسبه می‌کند
    و در جدول اندیکاتور مقصد درج می‌کند (نسخه ریالی و دلاری).
    سیگنال‌ها ۱/۰ هستند، دقیقاً مطابق منطق قبلی پروژه‌ات.

    پارامترها:
        source_table: نام جدول داده‌ی روزانه (مثلاً daily_stock_data, daily_fund_gold, ...)
        dest_table:   نام جدول اندیکاتورها (مثلاً daily_indicators, daily_indicators_fund_gold, ...)
        insert_mode:  "upsert" (پیشنهادی) یا "replace_all" (حذف کل جدول و درج مجدد)
    """
    db_url = _resolve_db_url()
    # Engine برای pandas (رفع Warning)
    engine = create_engine(db_url)
    # اتصال psycopg2 برای درج سریع
    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:

        # لیست نمادها از جدول منبع
        cur.execute(f"SELECT DISTINCT stock_ticker FROM {source_table} WHERE stock_ticker IS NOT NULL")
        tickers = [r[0] for r in cur.fetchall()]
        if not tickers:
            print(f"⚠️ نمادی در {source_table} پیدا نشد.")
            return

        total_rows = 0

        for i, t in enumerate(tickers, 1):
            print(f"[{i}/{len(tickers)}] ⏳ محاسبه اندیکاتور: {t} از {source_table}")

            df = pd.read_sql_query(
                f"""
                SELECT stock_ticker, date_miladi,
                       adjust_open, adjust_high, adjust_low, adjust_close,
                       adjust_open_usd, adjust_high_usd, adjust_low_usd, adjust_close_usd,
                       dollar_rate
                FROM {source_table}
                WHERE stock_ticker = %(ticker)s
                ORDER BY date_miladi ASC
                """,
                con=engine,
                params={"ticker": t},
                parse_dates=["date_miladi"]
            )

            if df.empty:
                continue

            # سری‌های قیمتی (ریالی)
            close = pd.to_numeric(df["adjust_close"], errors="coerce")
            high  = pd.to_numeric(df["adjust_high"], errors="coerce")
            low   = pd.to_numeric(df["adjust_low"],  errors="coerce")

            # ---------------- ریالی ----------------
            ema20  = _ema(close, 20)
            ema50  = _ema(close, 50)
            ema100 = _ema(close, 100)
            rsi14  = _rsi(close, 14)
            macd, macd_sig, macd_hist = _macd(close)
            tenkan, kijun, senkou_a, senkou_b, chikou = _ichimoku(high, low, close)
            atr22  = _atr(high, low, close, 22)

            # رنکو با جعبه=آخرین ATR22 غیرخالی
            last_box = atr22.dropna().iloc[-1] if not atr22.dropna().empty else np.nan
            renko_dir = _renko_direction(close, float(last_box) if pd.notna(last_box) else None)

            # سیگنال‌ها (ریالی)
            sig_ichimoku_buy  = ((tenkan > kijun) & (tenkan.shift(1) < kijun.shift(1))).astype("Int64").fillna(0)
            sig_ichimoku_sell = ((tenkan < kijun) & (tenkan.shift(1) > kijun.shift(1))).astype("Int64").fillna(0)

            sig_ema_cross_buy  = ((ema20.shift(1) < ema50.shift(1)) & (ema20 > ema50)).astype("Int64").fillna(0)
            sig_ema_cross_sell = ((ema20.shift(1) > ema50.shift(1)) & (ema20 < ema50)).astype("Int64").fillna(0)

            sig_rsi_buy  = ((rsi14.shift(1) < 30) & (rsi14 > 30)).astype("Int64").fillna(0)
            sig_rsi_sell = ((rsi14.shift(1) > 70) & (rsi14 < 70)).astype("Int64").fillna(0)

            sig_macd_buy  = ((macd.shift(1) < macd_sig.shift(1)) & (macd > macd_sig)).astype("Int64").fillna(0)
            sig_macd_sell = ((macd.shift(1) > macd_sig.shift(1)) & (macd < macd_sig)).astype("Int64").fillna(0)

            sig_ema50_100_buy  = ((ema50.shift(1) < ema100.shift(1)) & (ema50 > ema100) & (ema20 > ema100)).astype("Int64").fillna(0)
            sig_ema50_100_sell = ((ema50.shift(1) > ema100.shift(1)) & (ema50 < ema100) & (ema20 < ema100)).astype("Int64").fillna(0)

            # ---------------- دلاری ----------------
            close_d = pd.to_numeric(df["adjust_close_usd"], errors="coerce")
            high_d  = pd.to_numeric(df["adjust_high_usd"],  errors="coerce")
            low_d   = pd.to_numeric(df["adjust_low_usd"],   errors="coerce")

            ema20_d  = _ema(close_d, 20)
            ema50_d  = _ema(close_d, 50)
            ema100_d = _ema(close_d, 100)
            rsi14_d  = _rsi(close_d, 14)
            macd_d, macd_sig_d, macd_hist_d = _macd(close_d)
            tenkan_d, kijun_d, senkou_a_d, senkou_b_d, chikou_d = _ichimoku(high_d, low_d, close_d)
            atr22_d = _atr(high_d, low_d, close_d, 22)

            last_box_d = atr22_d.dropna().iloc[-1] if not atr22_d.dropna().empty else np.nan
            renko_dir_d = _renko_direction(close_d, float(last_box_d) if pd.notna(last_box_d) else None)

            sig_ichimoku_buy_d  = ((tenkan_d > kijun_d) & (tenkan_d.shift(1) < kijun_d.shift(1))).astype("Int64").fillna(0)
            sig_ichimoku_sell_d = ((tenkan_d < kijun_d) & (tenkan_d.shift(1) > kijun_d.shift(1))).astype("Int64").fillna(0)

            sig_ema_cross_buy_d  = ((ema20_d.shift(1) < ema50_d.shift(1)) & (ema20_d > ema50_d)).astype("Int64").fillna(0)
            sig_ema_cross_sell_d = ((ema20_d.shift(1) > ema50_d.shift(1)) & (ema20_d < ema50_d)).astype("Int64").fillna(0)

            sig_rsi_buy_d  = ((rsi14_d.shift(1) < 30) & (rsi14_d > 30)).astype("Int64").fillna(0)
            sig_rsi_sell_d = ((rsi14_d.shift(1) > 70) & (rsi14_d < 70)).astype("Int64").fillna(0)

            sig_macd_buy_d  = ((macd_d.shift(1) < macd_sig_d.shift(1)) & (macd_d > macd_sig_d)).astype("Int64").fillna(0)
            sig_macd_sell_d = ((macd_d.shift(1) > macd_sig_d.shift(1)) & (macd_d < macd_sig_d)).astype("Int64").fillna(0)

            sig_ema50_100_buy_d  = ((ema50_d.shift(1) < ema100_d.shift(1)) & (ema50_d > ema100_d) & (ema20_d > ema100_d)).astype("Int64").fillna(0)
            sig_ema50_100_sell_d = ((ema50_d.shift(1) > ema100_d.shift(1)) & (ema50_d < ema100_d) & (ema20_d < ema100_d)).astype("Int64").fillna(0)

            # ساخت رکوردها
            block = pd.DataFrame({
                "stock_ticker": df["stock_ticker"].values,
                "date_miladi":  df["date_miladi"].values,

                "ema_20": ema20, "ema_50": ema50, "ema_100": ema100,
                "rsi": rsi14, "macd": macd, "macd_signal": macd_sig, "macd_hist": macd_hist,
                "tenkan": tenkan, "kijun": kijun, "senkou_a": senkou_a, "senkou_b": senkou_b, "chikou": chikou,
                "signal_ichimoku_buy":  sig_ichimoku_buy,  "signal_ichimoku_sell": sig_ichimoku_sell,
                "signal_ema_cross_buy": sig_ema_cross_buy, "signal_ema_cross_sell": sig_ema_cross_sell,
                "signal_rsi_buy":       sig_rsi_buy,       "signal_rsi_sell":      sig_rsi_sell,
                "signal_macd_buy":      sig_macd_buy,      "signal_macd_sell":     sig_macd_sell,
                "signal_ema50_100_buy": sig_ema50_100_buy, "signal_ema50_100_sell": sig_ema50_100_sell,
                "atr_22": atr22, "renko_22": renko_dir,

                "ema_20_d": ema20_d, "ema_50_d": ema50_d, "ema_100_d": ema100_d,
                "rsi_d": rsi14_d, "macd_d": macd_d, "macd_signal_d": macd_sig_d, "macd_hist_d": macd_hist_d,
                "tenkan_d": tenkan_d, "kijun_d": kijun_d, "senkou_a_d": senkou_a_d, "senkou_b_d": senkou_b_d, "chikou_d": chikou_d,
                "signal_ichimoku_buy_d":  sig_ichimoku_buy_d,  "signal_ichimoku_sell_d": sig_ichimoku_sell_d,
                "signal_ema_cross_buy_d": sig_ema_cross_buy_d, "signal_ema_cross_sell_d": sig_ema_cross_sell_d,
                "signal_rsi_buy_d":       sig_rsi_buy_d,       "signal_rsi_sell_d":      sig_rsi_sell_d,
                "signal_macd_buy_d":      sig_macd_buy_d,      "signal_macd_sell_d":     sig_macd_sell_d,
                "signal_ema50_100_buy_d": sig_ema50_100_buy_d, "signal_ema50_100_sell_d": sig_ema50_100_sell_d,
                "atr_22_d": atr22_d, "renko_22_d": renko_dir_d,
            })

            # حذف سطرهای بدون تاریخ/تیکر
            block = block.dropna(subset=["date_miladi", "stock_ticker"])

            # تاریخ‌ها را به datetime.date تبدیل کن (برای جلوگیری از خطای DATE vs bigint)
            block["date_miladi"] = pd.to_datetime(block["date_miladi"]).dt.date

            # NaN -> None برای psycopg2 و numpy types -> python
            for c in block.columns:
                block[c] = block[c].apply(_py)

            # درج: یا replace_all یا upsert
            # === قبل از درج: ستون‌ها و ردیف‌ها را کاملاً پایتونی کن ===
            cols = [
                "stock_ticker", "date_miladi",
                "ema_20", "ema_50", "ema_100",
                "rsi", "macd", "macd_signal", "macd_hist",
                "tenkan", "kijun", "senkou_a", "senkou_b", "chikou",
                "signal_ichimoku_buy", "signal_ichimoku_sell",
                "signal_ema_cross_buy", "signal_ema_cross_sell",
                "signal_rsi_buy", "signal_rsi_sell",
                "signal_macd_buy", "signal_macd_sell",
                "signal_ema50_100_buy", "signal_ema50_100_sell",
                "atr_22", "renko_22",
                "ema_20_d", "ema_50_d", "ema_100_d",
                "rsi_d", "macd_d", "macd_signal_d", "macd_hist_d",
                "tenkan_d", "kijun_d", "senkou_a_d", "senkou_b_d", "chikou_d",
                "signal_ichimoku_buy_d", "signal_ichimoku_sell_d",
                "signal_ema_cross_buy_d", "signal_ema_cross_sell_d",
                "signal_rsi_buy_d", "signal_rsi_sell_d",
                "signal_macd_buy_d", "signal_macd_sell_d",
                "signal_ema50_100_buy_d", "signal_ema50_100_sell_d",
                "atr_22_d", "renko_22_d"
            ]

            # فقط همین ستون‌ها را نگه دار
            blk = block[cols].copy()

            # تاریخ‌ها از قبل به datetime.date تبدیل شده‌اند؛
            # اینجا همه ستون‌ها را به object ببریم و NaN/NA را None کنیم
            blk = blk.astype(object).where(pd.notnull(blk), None)

            # تبدیل تمام سلول‌ها به انواع پایتونی (int/float/str/None)
            blk = blk.applymap(_py)

            # حالا rows به‌صورت لیست تاپل‌های پایتونی
            rows = [tuple(row) for row in blk.to_numpy(dtype=object)]

            if not rows:
                continue

            if insert_mode == "replace_all":
                cur.execute(f"DELETE FROM {dest_table} WHERE stock_ticker = %s", (t,))
                insert_sql = f"INSERT INTO {dest_table} ({','.join(cols)}) VALUES %s"
                execute_values(cur, insert_sql, rows, page_size=1000)
            else:
                insert_sql = f"""
                INSERT INTO {dest_table} ({','.join(cols)}) VALUES %s
                ON CONFLICT (stock_ticker, date_miladi) DO UPDATE SET
                  ema_20=EXCLUDED.ema_20, ema_50=EXCLUDED.ema_50, ema_100=EXCLUDED.ema_100,
                  rsi=EXCLUDED.rsi, macd=EXCLUDED.macd, macd_signal=EXCLUDED.macd_signal, macd_hist=EXCLUDED.macd_hist,
                  tenkan=EXCLUDED.tenkan, kijun=EXCLUDED.kijun, senkou_a=EXCLUDED.senkou_a, senkou_b=EXCLUDED.senkou_b, chikou=EXCLUDED.chikou,
                  signal_ichimoku_buy=EXCLUDED.signal_ichimoku_buy, signal_ichimoku_sell=EXCLUDED.signal_ichimoku_sell,
                  signal_ema_cross_buy=EXCLUDED.signal_ema_cross_buy, signal_ema_cross_sell=EXCLUDED.signal_ema_cross_sell,
                  signal_rsi_buy=EXCLUDED.signal_rsi_buy, signal_rsi_sell=EXCLUDED.signal_rsi_sell,
                  signal_macd_buy=EXCLUDED.signal_macd_buy, signal_macd_sell=EXCLUDED.signal_macd_sell,
                  signal_ema50_100_buy=EXCLUDED.signal_ema50_100_buy, signal_ema50_100_sell=EXCLUDED.signal_ema50_100_sell,
                  atr_22=EXCLUDED.atr_22, renko_22=EXCLUDED.renko_22,
                  ema_20_d=EXCLUDED.ema_20_d, ema_50_d=EXCLUDED.ema_50_d, ema_100_d=EXCLUDED.ema_100_d,
                  rsi_d=EXCLUDED.rsi_d, macd_d=EXCLUDED.macd_d, macd_signal_d=EXCLUDED.macd_signal_d, macd_hist_d=EXCLUDED.macd_hist_d,
                  tenkan_d=EXCLUDED.tenkan_d, kijun_d=EXCLUDED.kijun_d, senkou_a_d=EXCLUDED.senkou_a_d, senkou_b_d=EXCLUDED.senkou_b_d, chikou_d=EXCLUDED.chikou_d,
                  signal_ichimoku_buy_d=EXCLUDED.signal_ichimoku_buy_d, signal_ichimoku_sell_d=EXCLUDED.signal_ichimoku_sell_d,
                  signal_ema_cross_buy_d=EXCLUDED.signal_ema_cross_buy_d, signal_ema_cross_sell_d=EXCLUDED.signal_ema_cross_sell_d,
                  signal_rsi_buy_d=EXCLUDED.signal_rsi_buy_d, signal_rsi_sell_d=EXCLUDED.signal_rsi_sell_d,
                  signal_macd_buy_d=EXCLUDED.signal_macd_buy_d, signal_macd_sell_d=EXCLUDED.signal_macd_sell_d,
                  signal_ema50_100_buy_d=EXCLUDED.signal_ema50_100_buy_d, signal_ema50_100_sell_d=EXCLUDED.signal_ema50_100_sell_d,
                  atr_22_d=EXCLUDED.atr_22_d, renko_22_d=EXCLUDED.renko_22_d
                """
                execute_values(cur, insert_sql, rows, page_size=1000)

            total_rows += len(rows)

        conn.commit()
        print(f"✅ {total_rows} ردیف در {dest_table} درج/به‌روزرسانی شد.")
