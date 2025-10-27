# backend/api/orderbook.py
from enum import Enum
from collections import defaultdict
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import pandas as pd

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.sql_loader import load_sql
from backend.utils.response import create_response  # پاسخ واحد

router = APIRouter(prefix="/orderbook", tags=["📊 Orderbook"])

class Mode(str, Enum):
    sector = "sector"
    intra = "intra-sector"

@router.get("/bumpchart", summary="رتبه‌بندی لحظه‌ای خالص سفارش‌ها (Bump Chart)")
async def get_orderbook_bumpchart_data(
    mode: Mode = Query(Mode.sector, description="sector یا intra-sector"),
    sector: str | None = Query(None, description="نام صنعت در حالت intra-sector"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.OrderBook.BumpChart","ALL"))
):
    # اعتبارسنجی
    if mode == Mode.intra and not sector:
        raise HTTPException(status_code=400, detail="sector is required in intra-sector mode")

    # انتخاب SQL
    if mode == Mode.sector:
        sql = load_sql("orderbook_sector_timeseries")
        params = {}
        group_col = "sector"
    else:
        sql = load_sql("orderbook_intrasector_timeseries")
        params = {"sector": sector}
        group_col = "Symbol"

    # اجرای Async
    res = await db.execute(text(sql), params)
    rows = res.mappings().all()
    if not rows:
        return create_response(data=[], message="هیچ داده‌ای یافت نشد", status_code=200)

    df = pd.DataFrame(rows)

    # ✅ فیلتر فقط «داده‌های امروز» بر اساس ستون minute
    # - فرض بر این است که ستون minute از نوع datetime/timestamp است یا قابل تبدیل به آن.
    # - اگر TZ نداشت، همان تاریخ سیستم سرور مبنا قرار می‌گیرد.
    df["minute"] = pd.to_datetime(df["minute"], errors="coerce")
    today_date = pd.Timestamp.now().date()
    df = df[df["minute"].dt.date == today_date]

    # اگر بعد از فیلتر امروز چیزی نماند، پاسخ استاندارد بده
    if df.empty:
        return create_response(data=[], message="برای امروز داده‌ای موجود نیست", status_code=200)


    # ستون‌های موردنیاز
    need = {"total_buy", "total_sell", "minute", group_col}
    miss = need - set(df.columns)
    if miss:
        raise HTTPException(status_code=500, detail=f"Missing columns: {', '.join(miss)}")

    # خالص سفارش
    df["net_value"] = df["total_buy"] - df["total_sell"]
    df = df.fillna(0)

    # ساخت داده رتبه‌ها برای Bump Chart
    minutes = sorted(df["minute"].unique())
    groups = df[group_col].unique().tolist()
    bump = defaultdict(list)

    for m in minutes:
        tmp = df[df["minute"] == m].groupby(group_col)["net_value"].sum().reset_index()
        tmp = tmp.sort_values("net_value", ascending=False).reset_index(drop=True)
        tmp["rank"] = tmp.index + 1
        rank_map = dict(zip(tmp[group_col], tmp["rank"]))
        for g in groups:
            bump[g].append(int(rank_map[g]) if g in rank_map else None)

    # فوروارد/بکوارد فیل برای پر کردن None
    ranking_df = pd.DataFrame(bump, index=minutes).ffill().bfill()
    bump_filled = ranking_df.to_dict(orient="list")

    # خروجی استاندارد
    payload = {
        "minutes": [str(m) for m in minutes],
        "series": [{"name": g, "ranks": bump_filled[g]} for g in groups]
    }
    return create_response(data=payload, message="✅ Bump chart generated", status_code=200)
