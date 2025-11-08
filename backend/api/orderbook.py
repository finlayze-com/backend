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
import re  # 👈 برای حذف سمی‌کالن انتهایی SQL

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.sql_loader import load_sql
from backend.utils.response import create_response

router = APIRouter(prefix="/orderbook", tags=["📊 Orderbook"])

class Mode(str, Enum):
    sector = "sector"
    intra = "intra-sector"


@router.get("/bumpchart", summary="رتبه‌بندی لحظه‌ای خالص سفارش‌ها (Bump Chart)")
async def get_orderbook_bumpchart_data(
    mode: Mode = Query(Mode.sector, description="sector یا intra-sector"),
    sector: str | None = Query(None, description="نام صنعت در حالت intra-sector"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.OrderBook.BumpChart", "ALL"))
):
    # 1) اعتبارسنجی
    if mode == Mode.intra and not sector:
        raise HTTPException(status_code=400, detail="sector is required in intra-sector mode")

    # 2) SQL پایه + حذف سمی‌کالن انتهایی
    base_sql = load_sql("orderbook_sector_timeseries") if mode == Mode.sector else load_sql("orderbook_intrasector_timeseries")
    base_sql_clean = re.sub(r";\s*$", "", base_sql.strip())  # 👈 سمی‌کالن پایانی را بردار

    group_col = "sector" if mode == Mode.sector else "Symbol"

    # 3) بازه امروز تهران 09:00–13:00 (ستون minute tz-naive است → پارامترها هم tz-naive)
    now_teh = datetime.now(ZoneInfo("Asia/Tehran"))
    today_teh = now_teh.date()
    start_teh_aware = datetime.combine(today_teh, time(9, 0), tzinfo=ZoneInfo("Asia/Tehran"))
    end_teh_aware   = datetime.combine(today_teh, time(13, 0), tzinfo=ZoneInfo("Asia/Tehran"))
    start_naive = start_teh_aware.replace(tzinfo=None)
    end_naive   = end_teh_aware.replace(tzinfo=None)

    # 4) Wrap به‌صورت CTE + فیلتر بازه در SQL
    sql = f"""
    WITH src AS (
        {base_sql_clean}
    )
    SELECT *
    FROM src
    WHERE minute >= :start AND minute < :end
    """

    params = {"start": start_naive, "end": end_naive}
    if mode == Mode.intra:
        params["sector"] = sector

    # 5) اجرا
    res = await db.execute(text(sql), params)
    rows = res.mappings().all()
    if not rows:
        return create_response(data=[], message="❌ هیچ داده‌ای در بازه امروز (09:00 تا 13:00) یافت نشد", status_code=200)

    df = pd.DataFrame(rows)

    # 6) چک ستون‌ها
    need = {"total_buy", "total_sell", "minute", group_col}
    miss = need - set(df.columns)
    if miss:
        raise HTTPException(status_code=500, detail=f"Missing columns: {', '.join(miss)}")

    # 7) آماده‌سازی و محاسبات
    df["net_value"] = (df["total_buy"].fillna(0) - df["total_sell"].fillna(0)).astype(float)
    df["minute"] = pd.to_datetime(df["minute"], errors="coerce")
    df = df.dropna(subset=["minute"]).sort_values("minute")

    minutes = sorted(df["minute"].unique())
    if not minutes:
        return create_response(data=[], message="هیچ زمان معتبری یافت نشد", status_code=200)

    groups = df[group_col].astype(str).unique().tolist()

    tmp = (
        df.groupby(["minute", group_col], as_index=False)["net_value"].sum()
    )

    bump = defaultdict(list)
    for m in minutes:
        sm = tmp[tmp["minute"] == pd.Timestamp(m)]
        if sm.empty:
            for g in groups:
                bump[g].append(None)
            continue
        sm = sm.sort_values("net_value", ascending=False).reset_index(drop=True)
        sm["rank"] = sm.index + 1
        rank_map = dict(zip(sm[group_col].astype(str), sm["rank"]))
        for g in groups:
            bump[g].append(int(rank_map[g]) if g in rank_map else None)

    ranking_df = pd.DataFrame(bump, index=minutes).ffill().bfill()

    payload = {
        "minutes": [pd.Timestamp(m).strftime("%H:%M") for m in minutes],
        "series": [{"name": g, "ranks": ranking_df[g].tolist()} for g in groups]
    }
    return create_response(
        data=payload,
        message="✅ Bump chart فقط برای امروز (09:00 تا 13:00)",
        status_code=200
    )
