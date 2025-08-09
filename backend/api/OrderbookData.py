# backend/api/OrderbookData.py
from enum import Enum
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import pandas as pd

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.sql_loader import load_sql
from backend.utils.response import create_response  # ← ساختار پاسخ واحد

router = APIRouter(prefix="/orderbook", tags=["📊 Orderbook"])

class Mode(str, Enum):
    sector = "sector"
    intra = "intra-sector"

@router.get("/timeseries", summary="تایم‌سری ورود/خروج سفارش‌ها (سکتوری/درون‌سکتور)")
async def get_orderbook_timeseries(
    mode: Mode = Query(Mode.sector, description="sector یا intra-sector"),
    sector: str | None = Query(None, description="نام صنعت، فقط در حالت intra-sector لازم است"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.OrderBook.TimeSeries","ALL"))  # ← پرمیشن
):
    # اعتبارسنجی ورودی
    if mode == Mode.intra and not sector:
        raise HTTPException(status_code=400, detail="sector is required in intra-sector mode")

    # انتخاب کوئری
    if mode == Mode.sector:
        sql = load_sql("orderbook_sector_timeseries")
        params = {}
        group_col = "sector"
        success_msg = "✅ Orderbook timeseries (sector)"
    else:
        sql = load_sql("orderbook_intrasector_timeseries")
        params = {"sector": sector}
        group_col = "Symbol"
        success_msg = f"✅ Orderbook timeseries (intra-sector: {sector})"

    # اجرای Async
    result = await db.execute(text(sql), params)
    rows = result.mappings().all()
    if not rows:
        return create_response(
            data=[],
            message="هیچ داده‌ای یافت نشد",
            status_code=200
        )

    df = pd.DataFrame(rows)

    # محاسبه خالص ارزش سفارش
    required_cols = {"total_buy", "total_sell", "minute", group_col}
    missing = required_cols - set(df.columns)
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing columns: {', '.join(missing)}")

    df["net_value"] = df["total_buy"] - df["total_sell"]
    df = df.fillna(0)

    # فقط ستون‌های لازم برای LineChart در فرانت
    out = (
        df[["minute", group_col, "net_value"]]
        .rename(columns={group_col: "name"})
        .to_dict(orient="records")
    )

    return create_response(
        data=out,
        message=success_msg,
        status_code=200
    )
