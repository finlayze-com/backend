# -*- coding: utf-8 -*-
"""
Generic weekly builder from daily tables (modular, using precomputed USD columns from daily).
- No USD recomputation here; only aggregates daily-precomputed USD columns.
"""

import pandas as pd
from .loader import get_engine, load_table, get_last_week_end
from .writer import upsert_dataframe

def build_weekly_from_daily(
    src_table: str,
    dst_table: str,
    date_col: str = "date_miladi",
    symbol_col: str = "stock_ticker",

    # Rial price columns (daily)
    open_col="open", high_col="high", low_col="low", close_col="close",
    final_price_col="final_price",
    aopen_col="adjust_open", ahigh_col="adjust_high", alow_col="adjust_low",
    aclose_col="adjust_close", afinal_col="adjust_final_price",

    # Volume / Value (daily)
    volume_col="volume", value_col="value",

    # Precomputed USD columns (daily) — if exist, will be aggregated; otherwise ignored
    aopen_usd_col="adjust_open_usd",
    ahigh_usd_col="adjust_high_usd",
    alow_usd_col="adjust_low_usd",
    aclose_usd_col="adjust_close_usd",
    value_usd_col="value_usd",
    dollar_rate_col="dollar_rate",  # optional carry-forward (e.g., 'last' of week)

    extra_identity_cols=None,
    conflict_on=("stock_ticker", "week_end")
):
    eng = get_engine()
    print(f"🔄 Building weekly data for: {src_table} → {dst_table}")

    # last written week_end in destination
    last_week_end = get_last_week_end(eng, dst_table)

    # load daily table
    df = load_table(eng, src_table)
    if df.empty:
        print("⚠️ Daily source is empty.")
        return

    # basic cleaning
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col, symbol_col]).sort_values([symbol_col, date_col])

    # build an aggregation map dynamically (only for columns that actually exist)
    # OHLC rules: first/max/min/last on adjusted/unadjusted; sum on value/volume; USD same as Rial rules
    agg_map = {}

    # --- Rial columns
    if open_col in df.columns:        agg_map[open_col]  = "first"
    if high_col in df.columns:        agg_map[high_col]  = "max"
    if low_col in df.columns:         agg_map[low_col]   = "min"
    if close_col in df.columns:       agg_map[close_col] = "last"

    if final_price_col in df.columns: agg_map[final_price_col] = "last"
    if aopen_col in df.columns:       agg_map[aopen_col]  = "first"
    if ahigh_col in df.columns:       agg_map[ahigh_col]  = "max"
    if alow_col in df.columns:        agg_map[alow_col]   = "min"
    if aclose_col in df.columns:      agg_map[aclose_col] = "last"
    if afinal_col in df.columns:      agg_map[afinal_col] = "last"

    if volume_col in df.columns:      agg_map[volume_col] = "sum"
    if value_col in df.columns:       agg_map[value_col]  = "sum"

    # --- Precomputed USD columns (aggregate same style as Rial)
    if aopen_usd_col in df.columns:   agg_map[aopen_usd_col]  = "first"
    if ahigh_usd_col in df.columns:   agg_map[ahigh_usd_col]  = "max"
    if alow_usd_col in df.columns:    agg_map[alow_usd_col]   = "min"
    if aclose_usd_col in df.columns:  agg_map[aclose_usd_col] = "last"
    if value_usd_col in df.columns:   agg_map[value_usd_col]  = "sum"

    # optional: keep a representative weekly dollar_rate (e.g., last of week)
    if dollar_rate_col in df.columns: agg_map[dollar_rate_col] = "last"

    all_weekly = []

    # group per symbol, then resample by week
    for sym, g in df.groupby(symbol_col, sort=False):
        if g.empty:
            continue

        gd = g.set_index(date_col)

        weekly = gd.groupby(pd.Grouper(freq="W-FRI", label="left", closed="left")).agg(agg_map)
        weekly = weekly.reset_index().rename(columns={date_col: "week_start"})

        # drop empty OHLC weeks (prevent garbage)
        core_ohlc = [c for c in [open_col, high_col, low_col, close_col] if c in weekly.columns]
        if core_ohlc:
            weekly = weekly.dropna(subset=core_ohlc, how="all")

        if weekly.empty:
            continue

        weekly["week_end"] = weekly["week_start"] + pd.Timedelta(days=6)
        weekly[symbol_col] = sym

        # attach last known identity columns if requested
        if extra_identity_cols:
            for c in extra_identity_cols:
                if c in g.columns:
                    # pick last non-null value over the window
                    weekly[c] = g[c].ffill().iloc[-1]

        # # write only new weeks (strictly greater than last_week_end, if last_week_end not None)
        # if last_week_end is not None:
        #     weekly = weekly[weekly["week_end"] > last_week_end]


        # ✅ Insert only NEW completed weeks, but always include the latest week for UPDATE
        if not weekly.empty:
            max_week_end = weekly["week_end"].max()

            if last_week_end is not None:
                weekly = weekly[
                    (weekly["week_end"] > last_week_end) |  # weeks not in DB yet
                    (weekly["week_end"] == max_week_end)  # always update last week
                    ]

                # (اختیاری ولی تمیز): اگر max_week_end خیلی عقب‌تر از last_week_end بود، چیزی ننویس
                weekly = weekly[weekly["week_end"] >= last_week_end]

        if not weekly.empty:
            all_weekly.append(weekly)

    if not all_weekly:
        print("📭 No new weekly rows to insert.")
        return

    out = pd.concat(all_weekly, ignore_index=True)

    # ensure week_start/week_end are plain dates in DB
    out["week_start"] = pd.to_datetime(out["week_start"]).dt.date
    out["week_end"]   = pd.to_datetime(out["week_end"]).dt.date

    upsert_dataframe(out, eng, dst_table, conflict_cols=conflict_on)
    print(f"✅ {len(out)} rows upserted into '{dst_table}'.")
