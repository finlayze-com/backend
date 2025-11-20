# backend/api/candlestick.py
from fastapi import APIRouter, Query, Depends, HTTPException
from enum import Enum
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from backend.api.metadata import get_db          # ← همون get_db پروژه شما
from backend.users.dependencies import require_permissions
from backend.utils.response import create_response
from backend.utils.logger import logger

router = APIRouter(tags=["📈 Candlestick"])

class Timeframe(str, Enum):
    daily = "daily"
    weekly = "weekly"

class Currency(str, Enum):
    rial = "rial"
    dollar = "dollar"

@router.get("/candlestick/rawdata", summary="خامِ دیتای کندل برای ECharts")
async def get_rawdata_for_echarts(
    stock: str = Query(..., description="نماد، مثلا: فملی"),
    timeframe: Timeframe = Query(Timeframe.daily, description="daily یا weekly"),
    currency: Currency = Query(Currency.rial, description="rial یا dollar"),
    db: AsyncSession = Depends(get_db),
    _ = Depends(require_permissions("Report.CandlestickRaw","ALL"))   # ✅ پرمیشن
):
    try:
        table = "daily_joined_data" if timeframe == Timeframe.daily else "weekly_joined_data"
        date_col = "date_miladi" if timeframe == Timeframe.daily else "week_end"

        if currency == Currency.rial:
            # همان ستون‌های ریالی
            sql = text(f"""
                SELECT {date_col} AS date,
                       adjust_open AS open,
                       adjust_close AS close,
                       adjust_low AS low,
                       adjust_high AS high,
                       value AS volume
                FROM {table}
                WHERE stock_ticker = :stock
                ORDER BY {date_col}
            """)
        else:
            # حالت دلاری (ستون‌های دلاری پکیج شما)
            sql = text(f"""
                SELECT {date_col} AS date,
                       adjust_open_usd AS open,
                       adjust_close_usd AS close,
                       adjust_low_usd AS low,
                       adjust_high_usd AS high,
                       value_usd AS volume
                FROM {table}
                WHERE stock_ticker = :stock
                ORDER BY {date_col}
            """)

        result = await db.execute(sql, {"stock": stock})
        rows = result.mappings().all()

        # به فرمت خام ECharts: [date, open, close, low, high, volume]
        raw = []
        for r in rows:
            d = r["date"]
            # تاریخ را به "YYYY/MM/DD" تبدیل کنیم
            ds = d.strftime("%Y/%m/%d") if hasattr(d, "strftime") else str(d)
            raw.append([ds,
                        float(r["open"] or 0),
                        float(r["close"] or 0),
                        float(r["low"] or 0),
                        float(r["high"] or 0),
                        float(r["volume"] or 0)])

        logger.info(f"[Candlestick] {stock=} {timeframe=} {currency=} rows={len(raw)}")
        return create_response(200, "دادهٔ کندل با موفقیت برگردانده شد", raw)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("❌ خطا در دریافت دادهٔ کندل")
        # پاسخ واحد با کد 500
        return create_response(500, "خطا در دریافت دادهٔ کندل", {"error": str(e)})
