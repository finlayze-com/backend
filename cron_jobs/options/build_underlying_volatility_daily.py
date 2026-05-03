import os
import math
from decimal import Decimal
from dotenv import load_dotenv

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch


# =========================
# ENV
# =========================
dotenv_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(dotenv_path)

DB_URL = os.getenv("DB_URL_SYNC")

if not DB_URL:
    raise ValueError("DB_URL_SYNC not found in .env")


# =========================
# CONFIG
# =========================
FUND_TABLES = [
    "daily_fund_balanced",
    "daily_fund_fixincome",
    "daily_fund_gold",
    "daily_fund_index_stock",
    "daily_fund_leverage",
    "daily_fund_other",
    "daily_fund_segment",
    "daily_fund_stock",
    "daily_fund_zafran",
]

SOURCE_TABLES = [("daily_stock_data", "stock")] + [
    (table_name, table_name.replace("daily_", "")) for table_name in FUND_TABLES
]


# =========================
# HELPERS
# =========================
def normalize_text(val):
    if val is None:
        return None
    return str(val).strip().replace("ي", "ی").replace("ك", "ک")


def get_existing_column(table_name, candidates):
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
    """

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (table_name,))
            cols = {row[0] for row in cur.fetchall()}

    for c in candidates:
        if c in cols:
            return c

    raise ValueError(f"No matching column found in {table_name}")


def get_last_hv_date():
    query = "SELECT MAX(trade_date) FROM public.underlying_volatility_daily"

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()

    return row[0] if row and row[0] else None


# =========================
# DATA READ
# =========================
def read_source_table(table_name, asset_source, start_date=None):
    date_col = get_existing_column(table_name, ["date_miladi", "date", "trade_date"])
    ticker_col = get_existing_column(table_name, ["stock_ticker", "symbol", "ticker", "Ticker", "namad"])
    close_col = get_existing_column(table_name, ["adjust_close", "close", "Close", "final", "Final"])

    query = f"""
        SELECT
            "{date_col}"::date AS trade_date,
            "{ticker_col}"::text AS ticker,
            "{close_col}"::numeric AS close_price
        FROM public.{table_name}
        WHERE "{close_col}" IS NOT NULL
          AND "{close_col}" > 0
          AND "{ticker_col}" IS NOT NULL
          AND "{date_col}" IS NOT NULL
    """

    params = []

    if start_date:
        query += f' AND "{date_col}" >= %s'
        params.append(start_date)

    with psycopg2.connect(DB_URL) as conn:
        df = pd.read_sql(query, conn, params=params if params else None)

    if df.empty:
        print(f"[WARN] {table_name}: no rows")
        return df

    df["ticker"] = df["ticker"].apply(normalize_text)
    df["asset_source"] = asset_source

    print(f"[INFO] {table_name}: rows={len(df)}")
    return df


def build_all_underlying_prices(start_date=None):
    frames = []

    for table_name, asset_source in SOURCE_TABLES:
        try:
            df = read_source_table(table_name, asset_source, start_date=start_date)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"[ERROR] {table_name}: {e}")

    if not frames:
        raise ValueError("No data loaded")

    all_df = pd.concat(frames, ignore_index=True)

    # remove duplicates
    all_df = all_df.sort_values(["ticker", "trade_date", "asset_source"])
    all_df = all_df.drop_duplicates(subset=["ticker", "trade_date"], keep="first")

    return all_df


# =========================
# CALCULATION
# =========================
def calculate_hv(df):
    df = df.copy()
    df = df.sort_values(["ticker", "trade_date"])

    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df = df[df["close_price"] > 0]

    df["prev_close"] = df.groupby("ticker")["close_price"].shift(1)

    df["log_return"] = (df["close_price"] / df["prev_close"]).apply(
        lambda x: math.log(x) if pd.notna(x) and x > 0 else None
    )

    annual_factor = math.sqrt(252)

    for window in [10, 20, 30, 60]:
        df[f"hv_{window}d"] = (
            df.groupby("ticker")["log_return"]
            .rolling(window=window, min_periods=window)
            .std()
            .reset_index(level=0, drop=True)
            * annual_factor
        )

    return df


def to_decimal_or_none(x):
    if pd.isna(x):
        return None
    try:
        return Decimal(str(float(x)))
    except:
        return None


# =========================
# UPSERT
# =========================
def upsert_underlying_volatility(df):
    if df.empty:
        print("[WARN] no rows to insert")
        return

    cols = [
        "trade_date",
        "ticker",
        "asset_source",
        "close_price",
        "log_return",
        "hv_10d",
        "hv_20d",
        "hv_30d",
        "hv_60d",
    ]

    insert_sql = f"""
        INSERT INTO public.underlying_volatility_daily (
            {",".join(cols)},
            created_at,
            updated_at
        )
        VALUES (
            {",".join(["%s"] * len(cols))},
            NOW(),
            NOW()
        )
        ON CONFLICT (trade_date, ticker)
        DO UPDATE SET
            asset_source = EXCLUDED.asset_source,
            close_price = EXCLUDED.close_price,
            log_return = EXCLUDED.log_return,
            hv_10d = EXCLUDED.hv_10d,
            hv_20d = EXCLUDED.hv_20d,
            hv_30d = EXCLUDED.hv_30d,
            hv_60d = EXCLUDED.hv_60d,
            updated_at = NOW()
    """

    rows = []
    for _, row in df.iterrows():
        rows.append([
            row["trade_date"],
            row["ticker"],
            row["asset_source"],
            to_decimal_or_none(row["close_price"]),
            to_decimal_or_none(row["log_return"]),
            to_decimal_or_none(row["hv_10d"]),
            to_decimal_or_none(row["hv_20d"]),
            to_decimal_or_none(row["hv_30d"]),
            to_decimal_or_none(row["hv_60d"]),
        ])

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, rows, page_size=1000)
        conn.commit()

    print(f"[DONE] upsert rows={len(rows)}")


# =========================
# MAIN
# =========================
def main():
    print("[INFO] building underlying volatility daily...")

    last_date = get_last_hv_date()

    if last_date:
        print(f"[INFO] last HV date = {last_date}")

        # برای rolling window
        start_date = last_date - pd.Timedelta(days=70)
    else:
        print("[INFO] first run → full build")
        start_date = None

    prices = build_all_underlying_prices(start_date=start_date)

    print(f"[INFO] merged rows={len(prices)}")
    print(f"[INFO] tickers={prices['ticker'].nunique()}")

    hv_df = calculate_hv(prices)

    # فقط دیتای جدید
    if last_date:
        hv_df = hv_df[hv_df["trade_date"] > last_date]

    upsert_underlying_volatility(hv_df)

    print("[DONE] volatility build completed")


if __name__ == "__main__":
    main()