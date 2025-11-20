# backend/api/orderbook.py
# -*- coding: utf-8 -*-

from enum import Enum
from collections import defaultdict
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import pandas as pd
from datetime import datetime, time
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
    # --- اعتبارسنجی اولیه ---
    if mode == Mode.intra and not sector:
        raise HTTPException(status_code=400, detail="sector is required in intra-sector mode")

    # نرمالایز فقط ورودی کاربر
    norm_sector = normalize_persian(sector) if sector else None

    # --- پیدا کردن آخرین روز معاملاتی از orderbook_snapshot ---
    last_day_res = await db.execute(
        text('SELECT MAX("Timestamp"::date) AS d FROM orderbook_snapshot')
    )
    last_day = last_day_res.scalar()
    if not last_day:
        return create_response(
            data=[],
            message="❌ هیچ روز معاملاتی در جدول orderbook_snapshot یافت نشد.",
            status_code=200,
        )

    # --- Load SQL بر اساس mode ---
    sql_name = "orderbook_sector_timeseries" if mode == Mode.sector else "orderbook_intrasector_timeseries"
    base_sql = load_sql(sql_name)
    base_sql_clean = re.sub(r";\s*$", "", base_sql.strip())

    group_col = "sector" if mode == Mode.sector else "Symbol"

    # --- بازه زمانی روی آخرین روز معاملاتی (09:00 - 13:00) ---
    start_naive = datetime.combine(last_day, time(9, 0))
    end_naive   = datetime.combine(last_day, time(13, 0))

    sql = f"""
    WITH src AS (
        {base_sql_clean}
    )
    SELECT *
    FROM src
    WHERE minute >= :start AND minute < :end
    """

    params = {"start": start_naive, "end": end_naive}

    # 🔥 فقط در حالت intra-sector پارامتر sector به SQL پاس می‌دهیم
    if mode == Mode.intra:
        params["sector"] = sector

    # --- اجرای کوئری ---
    res = await db.execute(text(sql), params)
    rows = res.mappings().all()
    if not rows:
        return create_response(
            data=[],
            message="❌ هیچ داده‌ای در بازه زمانی آخرین روز معاملاتی (09:00 تا 13:00) یافت نشد.",
            status_code=200,
        )

    df = pd.DataFrame(rows)

    # --- نرمال‌سازی و فیلتر کردن sector در حالت intra ---
    if mode == Mode.intra and norm_sector:
        if "Sector" in df.columns:
            df["sector_norm"] = df["Sector"].astype(str).apply(normalize_persian)
        elif "sector" in df.columns:
            df["sector_norm"] = df["sector"].astype(str).apply(normalize_persian)
        else:
            df["sector_norm"] = None

        df = df[df["sector_norm"] == norm_sector]

        if df.empty:
            return create_response(
                data=[],
                message=f"بعد از نرمال‌سازی هیچ داده‌ای برای «{sector}» در آخرین روز معاملاتی یافت نشد.",
                status_code=200,
            )

    # --- 🔎 فیلتر instrument_type فقط در حالت intrasector ---
    if mode == Mode.intra:
        allowed_types = {"saham", "retail", "block", "rights_issue"}
        if "instrument_type" in df.columns:
            df["instrument_type"] = df["instrument_type"].astype(str).str.lower()
            df = df[df["instrument_type"].isin(allowed_types)]
            if df.empty:
                return create_response(
                    data=[],
                    message="هیچ نمادی با instrument_type معتبر (saham / retail / block / rights_issue) در آخرین روز معاملاتی یافت نشد.",
                    status_code=200,
                )
        # اگر ستون instrument_type نباشد، مثل قبل ادامه می‌دهیم و 500 نمی‌دهیم

    # --- ستون‌های لازم ---
    need = {"total_buy", "total_sell", "minute", group_col}
    miss = need - set(df.columns)
    if miss:
        raise HTTPException(status_code=500, detail=f"Missing columns: {', '.join(miss)}")

    # --- محاسبه net_value ---
    df["net_value"] = (df["total_buy"].fillna(0) - df["total_sell"].fillna(0)).astype(float)
    df["minute"] = pd.to_datetime(df["minute"], errors="coerce")
    df = df.dropna(subset=["minute"]).sort_values("minute")

    minutes = sorted(df["minute"].unique())
    if not minutes:
        return create_response(
            data=[],
            message="هیچ زمان معتبری در بازه‌ی 09:00 تا 13:00 آخرین روز معاملاتی یافت نشد.",
            status_code=200,
        )

    groups = df[group_col].astype(str).unique().tolist()

    # جمع net_value در هر دقیقه و هر گروه
    tmp = df.groupby(["minute", group_col], as_index=False)["net_value"].sum()

    # --- ساخت bump chart: rankها در طول زمان ---
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
        message="✅ Bump chart بر اساس آخرین روز معاملاتی (09:00 تا 13:00)",
        status_code=200,
    )
