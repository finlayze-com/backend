import os
import math
from decimal import Decimal
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch


dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path)
DB_URL = os.getenv("DB_URL_SYNC")

if not DB_URL:
    raise ValueError("DB_URL_SYNC not found in .env")


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

DEFAULT_RISK_FREE_RATE = Decimal("0.30")


def normalize_text(val):
    if val is None:
        return None
    return str(val).strip().replace("ي", "ی").replace("ك", "ک")


def safe_decimal(x):
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except Exception:
        return None


def to_float(x):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


# =========================
# Black-Scholes
# =========================

def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def bs_d1(S, K, T, r, sigma):
    return (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))


def black_scholes_price(S, K, T, r, sigma, option_type):
    if S is None or K is None or T is None or r is None or sigma is None:
        return None
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None

    d1 = bs_d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)

    if option_type == "call":
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)

    if option_type == "put":
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)

    return None


def implied_volatility_bisection(
    market_price,
    S,
    K,
    T,
    r,
    option_type,
    tol=1e-6,
    max_iter=200,
    sigma_low=1e-4,
    sigma_high=5.0,
):
    if market_price is None or S is None or K is None or T is None or r is None:
        return None

    if market_price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None

    low_price = black_scholes_price(S, K, T, r, sigma_low, option_type)
    high_price = black_scholes_price(S, K, T, r, sigma_high, option_type)

    if low_price is None or high_price is None:
        return None

    if market_price < low_price or market_price > high_price:
        return None

    low = sigma_low
    high = sigma_high

    for _ in range(max_iter):
        mid = (low + high) / 2.0
        mid_price = black_scholes_price(S, K, T, r, mid, option_type)

        if mid_price is None:
            return None

        error = mid_price - market_price

        if abs(error) < tol:
            return mid

        if mid_price < market_price:
            low = mid
        else:
            high = mid

    return (low + high) / 2.0


def bs_greeks(S, K, T, r, sigma, option_type):
    if S is None or K is None or T is None or r is None or sigma is None:
        return None, None, None, None

    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None, None, None, None

    d1 = bs_d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    pdf_d1 = norm_pdf(d1)

    gamma = pdf_d1 / (S * sigma * math.sqrt(T))
    vega = S * pdf_d1 * math.sqrt(T) / 100.0

    if option_type == "call":
        delta = norm_cdf(d1)
        theta_annual = (
            -(S * pdf_d1 * sigma) / (2 * math.sqrt(T))
            - r * K * math.exp(-r * T) * norm_cdf(d2)
        )
    elif option_type == "put":
        delta = norm_cdf(d1) - 1
        theta_annual = (
            -(S * pdf_d1 * sigma) / (2 * math.sqrt(T))
            + r * K * math.exp(-r * T) * norm_cdf(-d2)
        )
    else:
        return None, None, None, None

    theta_daily = theta_annual / 365.0

    return delta, gamma, theta_daily, vega


# =========================
# Basic calculations
# =========================

def calc_days_to_expiry(end_date_obj, trade_date_obj):
    if end_date_obj is None or trade_date_obj is None:
        return None
    return (end_date_obj - trade_date_obj).days


def calc_time_to_expiry_years(days_to_expiry):
    if days_to_expiry is None:
        return None
    try:
        return Decimal(str(days_to_expiry)) / Decimal("365")
    except Exception:
        return None


def calc_intrinsic_value(option_type, strike_price, spot_close):
    if option_type is None or strike_price is None or spot_close is None:
        return None

    strike_price = safe_decimal(strike_price)
    spot_close = safe_decimal(spot_close)

    if strike_price is None or spot_close is None:
        return None

    if option_type == "call":
        return max(spot_close - strike_price, Decimal("0"))
    if option_type == "put":
        return max(strike_price - spot_close, Decimal("0"))

    return None


def calc_extrinsic_value(option_close, intrinsic_value):
    if option_close is None or intrinsic_value is None:
        return None

    option_close = safe_decimal(option_close)
    intrinsic_value = safe_decimal(intrinsic_value)

    if option_close is None or intrinsic_value is None:
        return None

    return option_close - intrinsic_value


def calc_break_even(option_type, strike_price, option_close):
    if option_type is None or strike_price is None or option_close is None:
        return None

    strike_price = safe_decimal(strike_price)
    option_close = safe_decimal(option_close)

    if strike_price is None or option_close is None:
        return None

    if option_type == "call":
        return strike_price + option_close
    if option_type == "put":
        return strike_price - option_close

    return None


def calc_moneyness_raw(strike_price, spot_close):
    if strike_price is None or spot_close is None:
        return None

    strike_price = safe_decimal(strike_price)
    spot_close = safe_decimal(spot_close)

    if strike_price is None or spot_close is None:
        return None

    return spot_close - strike_price


def calc_moneyness_pct(strike_price, spot_close):
    if strike_price is None or spot_close in (None, 0, "0"):
        return None

    strike_price = safe_decimal(strike_price)
    spot_close = safe_decimal(spot_close)

    if strike_price is None or spot_close in (None, Decimal("0")):
        return None

    return (spot_close - strike_price) / spot_close


def calc_is_greeks_computable(option_close, spot_close, strike_price, days_to_expiry, option_type):
    return (
        option_close is not None
        and spot_close is not None
        and strike_price is not None
        and days_to_expiry is not None
        and days_to_expiry > 0
        and option_type in ("call", "put")
    )


# =========================
# DB helpers
# =========================

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

    raise ValueError(
        f"No matching column found in public.{table_name}. "
        f"Candidates checked: {candidates}. Existing columns: {sorted(cols)}"
    )


def get_all_common_trade_dates():
    stock_date_col = get_existing_column("daily_stock_data", ["date_miladi", "date", "trade_date"])
    option_date_col = get_existing_column("daily_option", ["date_miladi", "date", "trade_date"])

    query = f"""
        SELECT DISTINCT ds."{stock_date_col}"::date AS trade_date
        FROM public.daily_stock_data ds
        INNER JOIN public.daily_option do2
            ON ds."{stock_date_col}" = do2."{option_date_col}"
        ORDER BY trade_date
    """

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [row[0] for row in rows]


def get_option_base_rows():
    query = """
        SELECT
            stock_ticker AS option_ticker,
            underlying_ticker,
            ins_code,
            option_type,
            strike_price,
            end_date,
            begin_date,
            is_active
        FROM public.option_detail
        WHERE underlying_ticker IS NOT NULL
          AND stock_ticker IS NOT NULL
          AND strike_price IS NOT NULL
          AND end_date IS NOT NULL
    """

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [
        {
            "option_ticker": row[0],
            "underlying_ticker": row[1],
            "ins_code": row[2],
            "option_type": row[3],
            "strike_price": row[4],
            "end_date": row[5],
            "begin_date": row[6],
            "is_active": row[7],
        }
        for row in rows
    ]


def get_stock_close_map(table_name, target_date):
    date_col = get_existing_column(table_name, ["date_miladi", "date", "trade_date"])
    symbol_col = get_existing_column(table_name, ["symbol", "ticker", "stock_ticker", "Ticker", "namad"])
    close_col = get_existing_column(table_name, ["adjust_close", "close", "Close", "final", "Final"])

    query = f"""
        SELECT
            "{symbol_col}"::text,
            "{close_col}"
        FROM public.{table_name}
        WHERE "{date_col}" = %s
    """

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (target_date,))
            rows = cur.fetchall()

    exact_map = {}
    normalized_map = {}

    for symbol, close_price in rows:
        exact_map[symbol] = close_price
        normalized_map[normalize_text(symbol)] = close_price

    return exact_map, normalized_map


def merge_maps(base_exact, base_norm, new_exact, new_norm):
    for k, v in new_exact.items():
        if k not in base_exact:
            base_exact[k] = v
    for k, v in new_norm.items():
        if k not in base_norm:
            base_norm[k] = v
    return base_exact, base_norm


def get_all_underlying_close_maps(target_date):
    final_exact, final_norm = get_stock_close_map("daily_stock_data", target_date)

    for table_name in FUND_TABLES:
        exact_map, norm_map = get_stock_close_map(table_name, target_date)
        final_exact, final_norm = merge_maps(final_exact, final_norm, exact_map, norm_map)

    return final_exact, final_norm


def get_option_last_available_price_map(target_date):
    date_col = get_existing_column("daily_option", ["date_miladi", "date", "trade_date"])
    symbol_col = get_existing_column("daily_option", ["symbol", "ticker", "stock_ticker", "Ticker", "namad"])
    close_col = get_existing_column("daily_option", ["adjust_close", "close", "Close", "final", "Final"])

    query = f"""
        WITH ranked AS (
            SELECT
                "{symbol_col}"::text AS option_symbol,
                "{date_col}"::date AS price_date,
                "{close_col}" AS option_close,
                ROW_NUMBER() OVER (
                    PARTITION BY "{symbol_col}"
                    ORDER BY "{date_col}" DESC
                ) AS rn
            FROM public.daily_option
            WHERE "{date_col}" <= %s
        )
        SELECT
            option_symbol,
            price_date,
            option_close
        FROM ranked
        WHERE rn = 1
    """

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (target_date,))
            rows = cur.fetchall()

    exact_map = {}
    normalized_map = {}

    for symbol, price_date, option_close in rows:
        exact_map[symbol] = (price_date, option_close)
        normalized_map[normalize_text(symbol)] = (price_date, option_close)

    return exact_map, normalized_map


def get_close_value(symbol, exact_map, normalized_map):
    if symbol in exact_map:
        return exact_map[symbol]

    norm_symbol = normalize_text(symbol)
    return normalized_map.get(norm_symbol)


def upsert_option_greeks_daily(batch_rows):
    if not batch_rows:
        return

    cols = [
        "trade_date",
        "option_ticker",
        "underlying_ticker",
        "ins_code",
        "option_type",
        "strike_price",
        "end_date",
        "days_to_expiry",
        "time_to_expiry_years",
        "risk_free_rate",
        "spot_close",
        "option_close",
        "option_price_date",
        "option_price_age_days",
        "has_option_close",
        "has_spot_close",
        "is_greeks_computable",
        "iv_solved",
        "greeks_computed",
        "intrinsic_value",
        "extrinsic_value",
        "break_even",
        "moneyness_raw",
        "moneyness_pct",
        "iv",
        "delta",
        "gamma",
        "theta",
        "vega",
        "is_active",
    ]

    insert_sql = f"""
        INSERT INTO public.option_greeks_daily (
            {",".join(cols)},
            created_at,
            updated_at
        )
        VALUES (
            {",".join(["%s"] * len(cols))},
            NOW(),
            NOW()
        )
        ON CONFLICT (trade_date, option_ticker)
        DO UPDATE SET
            underlying_ticker = EXCLUDED.underlying_ticker,
            ins_code = EXCLUDED.ins_code,
            option_type = EXCLUDED.option_type,
            strike_price = EXCLUDED.strike_price,
            end_date = EXCLUDED.end_date,
            days_to_expiry = EXCLUDED.days_to_expiry,
            time_to_expiry_years = EXCLUDED.time_to_expiry_years,
            risk_free_rate = EXCLUDED.risk_free_rate,
            spot_close = EXCLUDED.spot_close,
            option_close = EXCLUDED.option_close,
            option_price_date = EXCLUDED.option_price_date,
            option_price_age_days = EXCLUDED.option_price_age_days,
            has_option_close = EXCLUDED.has_option_close,
            has_spot_close = EXCLUDED.has_spot_close,
            is_greeks_computable = EXCLUDED.is_greeks_computable,
            iv_solved = EXCLUDED.iv_solved,
            greeks_computed = EXCLUDED.greeks_computed,
            intrinsic_value = EXCLUDED.intrinsic_value,
            extrinsic_value = EXCLUDED.extrinsic_value,
            break_even = EXCLUDED.break_even,
            moneyness_raw = EXCLUDED.moneyness_raw,
            moneyness_pct = EXCLUDED.moneyness_pct,
            iv = EXCLUDED.iv,
            delta = EXCLUDED.delta,
            gamma = EXCLUDED.gamma,
            theta = EXCLUDED.theta,
            vega = EXCLUDED.vega,
            is_active = EXCLUDED.is_active,
            updated_at = NOW()
    """

    values = [[row.get(c) for c in cols] for row in batch_rows]

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, values, page_size=500)
        conn.commit()


def build_row(row, trade_date, spot_close_exact, spot_close_norm, option_price_exact, option_price_norm):
    option_ticker = row["option_ticker"]
    underlying_ticker = row["underlying_ticker"]
    option_type = row["option_type"]
    strike_price = row["strike_price"]
    end_date_obj = row["end_date"]
    begin_date_obj = row["begin_date"]

    if begin_date_obj is not None and trade_date < begin_date_obj:
        return None

    if end_date_obj is not None and trade_date > end_date_obj:
        return None

    option_price_info = get_close_value(option_ticker, option_price_exact, option_price_norm)
    spot_close = get_close_value(underlying_ticker, spot_close_exact, spot_close_norm)

    option_price_date = None
    option_price_age_days = None
    option_close = None

    if option_price_info is not None:
        option_price_date, option_close = option_price_info
        option_price_age_days = (trade_date - option_price_date).days

    has_option_close = option_close is not None
    has_spot_close = spot_close is not None

    days_to_expiry = calc_days_to_expiry(end_date_obj, trade_date)
    time_to_expiry_years = calc_time_to_expiry_years(days_to_expiry)
    risk_free_rate = DEFAULT_RISK_FREE_RATE

    intrinsic_value = calc_intrinsic_value(option_type, strike_price, spot_close)
    extrinsic_value = calc_extrinsic_value(option_close, intrinsic_value)
    break_even = calc_break_even(option_type, strike_price, option_close)
    moneyness_raw = calc_moneyness_raw(strike_price, spot_close)
    moneyness_pct = calc_moneyness_pct(strike_price, spot_close)

    is_greeks_computable = calc_is_greeks_computable(
        option_close=option_close,
        spot_close=spot_close,
        strike_price=strike_price,
        days_to_expiry=days_to_expiry,
        option_type=option_type,
    )

    iv = None
    delta = None
    gamma = None
    theta = None
    vega = None
    iv_solved = False
    greeks_computed = False

    if is_greeks_computable:
        S = to_float(spot_close)
        K = to_float(strike_price)
        T = to_float(time_to_expiry_years)
        r = to_float(risk_free_rate)
        market_price = to_float(option_close)

        iv_float = implied_volatility_bisection(
            market_price=market_price,
            S=S,
            K=K,
            T=T,
            r=r,
            option_type=option_type,
        )

        if iv_float is not None:
            iv = Decimal(str(iv_float))
            iv_solved = True

            delta_f, gamma_f, theta_f, vega_f = bs_greeks(
                S=S,
                K=K,
                T=T,
                r=r,
                sigma=iv_float,
                option_type=option_type,
            )

            if all(x is not None for x in [delta_f, gamma_f, theta_f, vega_f]):
                delta = Decimal(str(delta_f))
                gamma = Decimal(str(gamma_f))
                theta = Decimal(str(theta_f))
                vega = Decimal(str(vega_f))
                greeks_computed = True

    return {
        "trade_date": trade_date,
        "option_ticker": option_ticker,
        "underlying_ticker": underlying_ticker,
        "ins_code": row["ins_code"],
        "option_type": option_type,
        "strike_price": strike_price,
        "end_date": end_date_obj,
        "days_to_expiry": days_to_expiry,
        "time_to_expiry_years": time_to_expiry_years,
        "risk_free_rate": risk_free_rate,
        "spot_close": spot_close,
        "option_close": option_close,
        "option_price_date": option_price_date,
        "option_price_age_days": option_price_age_days,
        "has_option_close": has_option_close,
        "has_spot_close": has_spot_close,
        "is_greeks_computable": is_greeks_computable,
        "iv_solved": iv_solved,
        "greeks_computed": greeks_computed,
        "intrinsic_value": intrinsic_value,
        "extrinsic_value": extrinsic_value,
        "break_even": break_even,
        "moneyness_raw": moneyness_raw,
        "moneyness_pct": moneyness_pct,
        "iv": iv,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "is_active": row["is_active"],
    }


def build_option_greeks_history():
    trade_dates = get_all_common_trade_dates()
    option_rows = get_option_base_rows()

    print(f"[INFO] total trade_dates = {len(trade_dates)}")
    print(f"[INFO] total options = {len(option_rows)}")

    total_inserted = 0

    for idx, trade_date in enumerate(trade_dates, start=1):
        print(f"\n[{idx}/{len(trade_dates)}] Processing date: {trade_date}")

        spot_close_exact, spot_close_norm = get_all_underlying_close_maps(trade_date)
        option_price_exact, option_price_norm = get_option_last_available_price_map(trade_date)

        batch = []

        for row in option_rows:
            built = build_row(
                row=row,
                trade_date=trade_date,
                spot_close_exact=spot_close_exact,
                spot_close_norm=spot_close_norm,
                option_price_exact=option_price_exact,
                option_price_norm=option_price_norm,
            )

            if built is not None:
                batch.append(built)

        upsert_option_greeks_daily(batch)
        total_inserted += len(batch)

        computed = sum(1 for x in batch if x["greeks_computed"])
        iv_solved = sum(1 for x in batch if x["iv_solved"])

        print(f"[DONE] {trade_date}: rows={len(batch)} | iv_solved={iv_solved} | greeks={computed}")

    print(f"\n[FINISHED] total rows inserted/updated = {total_inserted}")


if __name__ == "__main__":
    build_option_greeks_history()