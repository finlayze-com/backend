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

    # ✅ فیلتر فقط «داده‌های امروز از ساعت 08:30 به بعد»
    # - اگر ستون minute tz-naive باشد (timestamp without time zone) و به وقت تهران ذخیره شده:
    #   آن را به صورت محلی به Asia/Tehran نسبت می‌دهیم (بدون تغییر مقدار ظاهری).
    # - سپس بازه امروزِ تهران [08:30, 24:00) را نگه می‌داریم.
    df["minute"] = pd.to_datetime(df["minute"], errors="coerce")
    # تبدیل به زمانِ آگاه از منطقه زمانی تهران (localize) - مقدار لحظه را تغییر نمی‌دهد، فقط TZ اضافه می‌کند
    df["minute_local"] = df["minute"].dt.tz_localize("Asia/Tehran", nonexistent="shift_forward", ambiguous="NaT")

    tehran_now = pd.Timestamp.now(tz="Asia/Tehran")
    today_teh = tehran_now.normalize()                             # 00:00 امروز به وقت تهران
    start_teh = today_teh + pd.Timedelta(hours=8, minutes=30)      # 08:30 امروز
    end_teh   = today_teh + pd.Timedelta(days=1)                   # 00:00 فردا

    # فقط ردیف‌هایی که در بازه‌ی امروز تهران و از 08:30 به بعد هستند
    df = df[(df["minute_local"] >= start_teh) & (df["minute_local"] < end_teh)]

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
    # ⬅️ برای هم‌خوانی با فیلتر، محور زمانی را از minute_local می‌سازیم.
    minutes = sorted(df["minute_local"].unique())
    groups = df[group_col].unique().tolist()
    bump = defaultdict(list)

    for m in minutes:
        tmp = df[df["minute_local"] == m].groupby(group_col)["net_value"].sum().reset_index()
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
        # اگر خروجی ساده‌تر می‌خواهی، می‌توانی فقط ساعت/دقیقه بدهی:
        # "minutes": [pd.Timestamp(m).strftime("%H:%M") for m in minutes],
        "minutes": [str(m) for m in minutes],
        "series": [{"name": g, "ranks": bump_filled[g]} for g in groups]
    }
    return create_response(data=payload, message="✅ Bump chart generated", status_code=200)
