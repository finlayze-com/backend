# backend/api/orderbook.py
# -*- coding: utf-8 -*-

from enum import Enum
from collections import defaultdict
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import pandas as pd
from datetime import datetime, time
from zoneinfo import ZoneInfo
import re

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.sql_loader import load_sql
from backend.utils.response import create_response


router = APIRouter(prefix="/orderbook", tags=["📊 Orderbook"])


class Mode(str, Enum):
    sector = "sector"
    intra = "intra-sector"


def normalize_persian(t: str | None):
    """Normalize Persian/Arabic characters."""
    if t is None:
        return None
    if not isinstance(t, str):
        t = str(t)
    t = t.strip().lower()
    return (
        t.replace("ي", "ی")
         .replace("ك", "ک")
         .replace("\u200c", "")
         .replace("ـ", "")
    )


@router.get("/bumpchart", summary="رتبه‌بندی لحظه‌ای خالص سفارش‌ها (Bump Chart)")
async def get_orderbook_bumpchart_data(
    mode: Mode = Query(Mode.sector),
    sector: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.OrderBook.BumpChart", "ALL")),
):

    if mode == Mode.intra and not sector:
        raise HTTPException(status_code=400, detail="sector is required in intra-sector mode")

    # نرمالایز فقط ورودی کاربر
    norm_sector = normalize_persian(sector) if sector else None

    # --- Load SQL ---
    base_sql = load_sql("orderbook_sector_timeseries") if mode == Mode.sector else load_sql("orderbook_intrasector_timeseries")
    base_sql_clean = re.sub(r";\s*$", "", base_sql.strip())

    group_col = "sector" if mode == Mode.sector else "Symbol"

    # # --- Prepare time range (09:00 - 13:00 Tehran) ---
    #now = datetime.now(ZoneInfo("Asia/Tehran"))
    #today = now.date()

    # ✔ پیدا کردن آخرین روز معاملاتی واقعی در orderbook_snapshot
    last_day_res = await db.execute(
        text('SELECT MAX("Timestamp"::date) AS d FROM orderbook_snapshot')
    )
    today = last_day_res.scalar()

    start_naive = datetime.combine(today, time(9, 0), tzinfo=ZoneInfo("Asia/Tehran")).replace(tzinfo=None)
    end_naive   = datetime.combine(today, time(13, 0), tzinfo=ZoneInfo("Asia/Tehran")).replace(tzinfo=None)

    sql = f"""
    WITH src AS (
        {base_sql_clean}
    )
    SELECT *
    FROM src
    WHERE minute >= :start AND minute < :end
    """

    params = {"start": start_naive, "end": end_naive}

    # 🔥 مهم: این‌جا sector خام را بفرست، نه نرمالایز شده!
    if mode == Mode.intra:
        params["sector"] = sector

    # --- Run query ---
    res = await db.execute(text(sql), params)
    rows = res.mappings().all()
    if not rows:
        return create_response(data=[], message="❌ هیچ داده‌ای یافت نشد", status_code=200)

    df = pd.DataFrame(rows)

    # --- نرمال‌سازی روی df و فیلتر امن ---
    if mode == Mode.intra and norm_sector:
        if "sector" in df.columns:
            df["sector_norm"] = df["sector"].astype(str).apply(normalize_persian)
            df = df[df["sector_norm"] == norm_sector]

            if df.empty:
                return create_response(
                    data=[],
                    message=f"بعد از نرمال‌سازی هیچ داده‌ای برای «{sector}» یافت نشد",
                    status_code=200,
                )

    # --- Required columns ---
    need = {"total_buy", "total_sell", "minute", group_col}
    miss = need - set(df.columns)
    if miss:
        raise HTTPException(status_code=500, detail=f"Missing columns: {', '.join(miss)}")

    # --- Compute net_value ---
    df["net_value"] = (df["total_buy"].fillna(0) - df["total_sell"].fillna(0)).astype(float)
    df["minute"] = pd.to_datetime(df["minute"], errors="coerce")
    df = df.dropna(subset=["minute"]).sort_values("minute")

    minutes = sorted(df["minute"].unique())
    groups = df[group_col].astype(str).unique().tolist()

    tmp = df.groupby(["minute", group_col], as_index=False)["net_value"].sum()

    bump = defaultdict(list)
    for m in minutes:
        slice_m = tmp[tmp["minute"] == pd.Timestamp(m)]
        if slice_m.empty:
            for g in groups:
                bump[g].append(None)
            continue

        slice_m = slice_m.sort_values("net_value", ascending=False).reset_index(drop=True)
        slice_m["rank"] = slice_m.index + 1
        rank_map = dict(zip(slice_m[group_col].astype(str), slice_m["rank"]))

        for g in groups:
            bump[g].append(rank_map.get(g))

    ranking_df = pd.DataFrame(bump, index=minutes).ffill().bfill()

    payload = {
        "minutes": [pd.Timestamp(m).strftime("%H:%M") for m in minutes],
        "series": [{"name": g, "ranks": ranking_df[g].tolist()} for g in groups],
    }

    return create_response(
        data=payload,
        message="✅ Bump chart فقط برای امروز (09:00 تا 13:00)",
        status_code=200,
    )
