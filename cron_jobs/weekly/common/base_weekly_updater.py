# -*- coding: utf-8 -*-
"""
Generic weekly builder from daily tables (modular version using loader & writer).
"""

import pandas as pd
from sqlalchemy import create_engine
from .loader import get_engine, load_table, load_dollar_data, get_last_week_end
from .writer import upsert_dataframe

def build_weekly_from_daily(
    src_table: str,
    dst_table: str,
    date_col: str = "date_miladi",
    symbol_col: str = "stock_ticker",
    open_col="open", high_col="high", low_col="low", close_col="close",
    final_price_col="final_price",
    aopen_col="adjust_open", ahigh_col="adjust_high", alow_col="adjust_low",
    aclose_col="adjust_close", afinal_col="adjust_final_price",
    volume_col="volume", value_col="value",
    extra_identity_cols=None,
    conflict_on=("stock_ticker", "week_end")
):
    engine = get_engine()
    print(f"🔄 Building weekly data for: {src_table} → {dst_table}")

    last_week_end = get_last_week_end(engine, dst_table)
    df_daily = load_table(engine, src_table)
    dollar_df = load_dollar_data(engine)

    if df_daily.empty:
        print("⚠️ Daily source is empty.")
        return

    df_daily[date_col] = pd.to_datetime(df_daily[date_col])
    all_weekly = []

    for sym in df_daily[symbol_col].unique():
        d = df_daily[df_daily[symbol_col] == sym].copy()
        d = d.sort_values(date_col).set_index(date_col)

        weekly = d.groupby(pd.Grouper(freq="W-FRI", label="left", closed="left")).agg({
            open_col: "first",
            high_col: "max",
            low_col: "min",
            close_col: "last",
            final_price_col: "last",
            aopen_col: "first",
            ahigh_col: "max",
            alow_col: "min",
            aclose_col: "last",
            afinal_col: "last",
            volume_col: "sum",
            value_col: "sum"
        }).reset_index().rename(columns={date_col: "week_start"})

        weekly = weekly.dropna(subset=[open_col, high_col, low_col, close_col])
        weekly["week_end"] = weekly["week_start"] + pd.Timedelta(days=6)
        weekly[symbol_col] = sym
        weekly = weekly[weekly["week_end"] > last_week_end]

        if extra_identity_cols:
            for c in extra_identity_cols:
                if c in d.columns:
                    weekly[c] = d[c].iloc[-1]

        weekly["dollar_rate"] = weekly["week_start"].apply(
            lambda x: dollar_df.loc[:x]["dollar_rate"].iloc[-1]
            if not dollar_df.loc[:x].empty else None
        )
        weekly = weekly.dropna(subset=["dollar_rate"])

        weekly["adjust_open_usd"]  = weekly[aopen_col]  / weekly["dollar_rate"]
        weekly["adjust_high_usd"]  = weekly[ahigh_col]  / weekly["dollar_rate"]
        weekly["adjust_low_usd"]   = weekly[alow_col]   / weekly["dollar_rate"]
        weekly["adjust_close_usd"] = weekly[aclose_col] / weekly["dollar_rate"]
        weekly["value_usd"]        = weekly[value_col]  / weekly["dollar_rate"]

        all_weekly.append(weekly)

    if not all_weekly:
        print("📭 No new weekly rows to insert.")
        return

    weekly_all = pd.concat(all_weekly, ignore_index=True)
    upsert_dataframe(weekly_all, engine, dst_table, conflict_cols=conflict_on)
