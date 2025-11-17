# cron_jobs/daily/common/base_updater.py
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from cron_jobs.daily.common.loader import get_price_history
from cron_jobs.daily.common.writer import insert_daily_rows

def run_group(instrument_type: str, dest_table: str):
    """
    instrument_type مثل: saham | rights_issue | retail | block | fund_stock | ... | bond
    dest_table مثل: daily_stock_data | daily_rights_issue | ... | daily_bond
    """
    # .env را بارگذاری کن
    dotenv_path = os.path.join(os.path.dirname(__file__), "../../../.env")
    load_dotenv(dotenv_path)
    db_url = os.getenv("DB_URL_SYNC")
    if not db_url:
        raise RuntimeError("DB_URL_SYNC not set in .env")

    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        # لیست نمادها از symboldetail
        cur.execute("""
            SELECT stock_ticker
            FROM symboldetail
            WHERE instrument_type = %s AND stock_ticker IS NOT NULL
        """, (instrument_type,))
        stocks = [r[0] for r in cur.fetchall()]

        # دلار
        dollar_df = pd.read_sql("SELECT date_miladi, close FROM dollar_data", conn)
        dollar_df.rename(columns={"close": "dollar_rate"}, inplace=True)
        dollar_df["date_miladi"] = pd.to_datetime(dollar_df["date_miladi"])

        total = 0
        for i, stock in enumerate(stocks, 1):
            print(f"[{i}/{len(stocks)}] ⏳ {instrument_type} :: {stock}")
            try:
                # آخرین تاریخ ذخیره‌شده برای این نماد در جدول مقصد
                cur.execute(
                    f"SELECT MAX(date_miladi) FROM {dest_table} WHERE stock_ticker=%s",
                    (stock,),
                )
                last_date = cur.fetchone()[0]

                # اگر سطر آخر temp بود، حذفش کن
                if last_date:
                    cur.execute(
                        f"""
                        DELETE FROM {dest_table}
                        WHERE stock_ticker=%s AND date_miladi=%s
                          AND (is_temp IS TRUE OR is_temp = TRUE)
                        """,
                        (stock, last_date),
                    )

                df = get_price_history(stock)
                if df is None or df.empty:
                    print(f"⚠️ داده‌ای برای {stock} نیست.")
                    continue

                # اگر last_date داریم، فقط رکوردهای جدیدتر/برابر را نگه داریم
                if last_date is not None:
                    df = df[df["gregorian_date"] >= pd.to_datetime(last_date)]
                    if df.empty:
                        print(f"📭 هیچ رکورد جدیدی برای {stock}.")
                        continue

                # الحاق دلار
                df = df.merge(
                    dollar_df, how="left", left_on="gregorian_date", right_on="date_miladi"
                )
                df["dollar_rate"] = df["dollar_rate"].fillna(method="ffill")

                # محاسبات USD
                df["adjust_open_usd"] = df["Adj Open"] / df["dollar_rate"]
                df["adjust_high_usd"] = df["Adj High"] / df["dollar_rate"]
                df["adjust_low_usd"] = df["Adj Low"] / df["dollar_rate"]
                df["adjust_close_usd"] = df["Adj Close"] / df["dollar_rate"]
                df["value_usd"] = df["Value"] / df["dollar_rate"]

                # تبدیل به رکوردها
                records = list(zip(
                    [stock] * len(df),
                    df["j_date"],
                    df["gregorian_date"],
                    df["Weekday"],
                    df["Open"], df["High"], df["Low"], df["Close"], df["Final"],
                    df["Volume"], df["Value"], df["No"], df["Name"], df["Market"],
                    df["Adj Open"], df["Adj High"], df["Adj Low"], df["Adj Close"], df["Adj Final"],
                    df["dollar_rate"], df["adjust_open_usd"], df["adjust_high_usd"],
                    df["adjust_low_usd"], df["adjust_close_usd"], df["value_usd"],
                ))
                insert_daily_rows(cur, dest_table, records)
                conn.commit()
                total += len(records)
                print(f"✅ ذخیره شد: {stock} -> {len(records)} رکورد")

            except Exception as e:
                conn.rollback()
                print(f"❌ خطا برای {stock}: {e}")

        print(f"🎯 گروه {instrument_type} تمام شد. تعداد کل رکوردهای جدید: {total}")

# ----------------- Helpers -----------------

def _load_db_url():
    dotenv_path = os.path.join(os.path.dirname(__file__), "../../../.env")
    load_dotenv(dotenv_path)
    db_url = os.getenv("DB_URL_SYNC")
    if not db_url:
        raise RuntimeError("DB_URL_SYNC not set in .env")
    return db_url


def _load_dollar_df(conn):
    dollar_df = pd.read_sql("SELECT date_miladi, close FROM dollar_data", conn)
    dollar_df.rename(columns={"close": "dollar_rate"}, inplace=True)
    dollar_df["date_miladi"] = pd.to_datetime(dollar_df["date_miladi"])
    return dollar_df

def _update_one_stock(cur, dollar_df: pd.DataFrame, stock: str, dest_table: str) -> int:
    """
    منطق اصلی آپدیت یک stock_ticker (همون چیزی که قبلاً داخل حلقه run_group بود).
    خروجی: تعداد رکوردهای جدید درج‌شده.
    """
    print(f"⏳ آپدیت {stock} در جدول {dest_table}")

    # آخرین تاریخ ذخیره‌شده برای این نماد در جدول مقصد
    cur.execute(
        f"SELECT MAX(date_miladi) FROM {dest_table} WHERE stock_ticker=%s",
        (stock,),
    )
    last_date = cur.fetchone()[0]

    # اگر سطر آخر temp بود، حذفش کن
    if last_date:
        cur.execute(
            f"""
            DELETE FROM {dest_table}
            WHERE stock_ticker=%s AND date_miladi=%s
              AND (is_temp IS TRUE OR is_temp = TRUE)
            """,
            (stock, last_date),
        )

    df = get_price_history(stock)
    if df is None or df.empty:
        print(f"⚠️ داده‌ای برای {stock} نیست.")
        return 0

    # اگر last_date داریم، فقط رکوردهای جدیدتر/برابر را نگه داریم
    if last_date is not None:
        df = df[df["gregorian_date"] >= pd.to_datetime(last_date)]
        if df.empty:
            print(f"📭 هیچ رکورد جدیدی برای {stock}.")
            return 0

    # الحاق دلار
    df = df.merge(
        dollar_df, how="left", left_on="gregorian_date", right_on="date_miladi"
    )
    df["dollar_rate"] = df["dollar_rate"].fillna(method="ffill")

    # محاسبات USD
    df["adjust_open_usd"] = df["Adj Open"] / df["dollar_rate"]
    df["adjust_high_usd"] = df["Adj High"] / df["dollar_rate"]
    df["adjust_low_usd"] = df["Adj Low"] / df["dollar_rate"]
    df["adjust_close_usd"] = df["Adj Close"] / df["dollar_rate"]
    df["value_usd"] = df["Value"] / df["dollar_rate"]

    # تبدیل به رکوردها
    records = list(zip(
        [stock] * len(df),
        df["j_date"],
        df["gregorian_date"],
        df["Weekday"],
        df["Open"], df["High"], df["Low"], df["Close"], df["Final"],
        df["Volume"], df["Value"], df["No"], df["Name"], df["Market"],
        df["Adj Open"], df["Adj High"], df["Adj Low"], df["Adj Close"], df["Adj Final"],
        df["dollar_rate"], df["adjust_open_usd"], df["adjust_high_usd"],
        df["adjust_low_usd"], df["adjust_close_usd"], df["value_usd"],
    ))

    if not records:
        print(f"📭 هیچ رکورد جدیدی برای {stock}.")
        return 0

    insert_daily_rows(cur, dest_table, records)
    print(f"✅ ذخیره شد: {stock} -> {len(records)} رکورد")
    return len(records)

# ----------- just one Stock -----------------

def run_for_stocks(stocks: list[str], dest_table: str):
    """
    نسخه‌ی جدید: فقط لیست خاصی از stock_tickerها را آپدیت می‌کند.
    این همان منطقی است که می‌خواهیم در API افزایش سرمایه استفاده کنیم.
    """
    if not stocks:
        print("⚠️ لیست تیکر خالی است.")
        return

    db_url = _load_db_url()
    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        dollar_df = _load_dollar_df(conn)

        total = 0
        for i, stock in enumerate(stocks, 1):
            print(f"[{i}/{len(stocks)}] ⏳ refresh :: {stock}")
            try:
                added = _update_one_stock(cur, dollar_df, stock, dest_table)
                conn.commit()
                total += added
            except Exception as e:
                conn.rollback()
                print(f"❌ خطا برای {stock}: {e}")

        print(f"🎯 آپدیت تیکرهای انتخابی تمام شد. تعداد کل رکوردهای جدید: {total}")