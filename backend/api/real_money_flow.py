from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.response import create_response
from backend.utils.logger import logger

router = APIRouter(prefix="", tags=["💧 Real Money Flow"])

@router.get("/real-money-flow/timeseries", summary="سری‌زمانی جریان پول حقیقی (بازار/صنعت/نماد)")
async def get_real_money_flow_timeseries(
    timeframe: str = Query("daily", enum=["daily", "weekly"]),
    level: str = Query("sector", enum=["market", "sector", "ticker"]),
    sector: str | None = Query(None, description="نام صنعت برای فیلتر سطح 'sector'"),
    ticker: str | None = Query(None, description="نماد برای سطح 'ticker' الزامی است"),
    currency: str = Query("rial", enum=["rial", "dollar"]),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.RealMoneyFlow"))  # 👈 مثل بقیه روت‌ها
):
    try:
        table = "daily_joined_data" if timeframe == "daily" else "weekly_joined_data"
        date_col = "j_date" if timeframe == "daily" else "week_end"

        # مقدار جریان: ریالی یا دلاری
        flow_expr = "(buy_i_value - sell_i_value)"
        if currency == "dollar":
            flow_expr = f"{flow_expr} / NULLIF(dollar_rate, 0)"

        params = {}
        if level == "market":
            query = f"""
                SELECT {date_col} AS date,
                       COALESCE(SUM({flow_expr}), 0) AS real_money_flow
                FROM {table}
                GROUP BY {date_col}
                ORDER BY {date_col}
            """

        elif level == "sector":
            base = f"""
                SELECT {date_col} AS date,
                       sector,
                       COALESCE(SUM({flow_expr}), 0) AS real_money_flow
                FROM {table}
                WHERE sector IS NOT NULL
            """
            if sector:
                base += " AND sector = :sector"
                params["sector"] = sector
            query = base + f" GROUP BY {date_col}, sector ORDER BY {date_col}, sector"

        elif level == "ticker":
            if not ticker:
                raise HTTPException(status_code=400, detail="پارامتر 'ticker' برای سطح 'ticker' الزامی است.")
            query = f"""
                SELECT {date_col} AS date,
                       stock_ticker AS ticker,
                       sector,
                       COALESCE({flow_expr}, 0) AS real_money_flow
                FROM {table}
                WHERE stock_ticker = :ticker
                ORDER BY {date_col}
            """
            params["ticker"] = ticker

        else:
            raise HTTPException(status_code=400, detail="سطح وارد شده معتبر نیست.")

        result = await db.execute(text(query), params)
        rows = [dict(r) for r in result.mappings().all()]

        return create_response(
            message="سری‌زمانی جریان پول حقیقی با موفقیت بازیابی شد.",
            data=rows,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("❌ خطا در دریافت سری‌زمانی جریان پول حقیقی")
        return create_response(
            message="خطا در پردازش درخواست جریان پول حقیقی.",
            data={},
            status_code=500
        )
