from fastapi import APIRouter, Query,Depends,HTTPException
from backend.db.connection import get_engine
from sqlalchemy import text
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from backend.db.connection import async_session
from backend.users.dependencies import require_permissions, get_subscription_dependencies
from backend.users import models, schemas
from backend.utils.response import create_response
from backend.utils.logger import logger

router = APIRouter()

# 🔧 Helper
async def get_db():
    async with async_session() as session:
        yield session


@router.get("/sectors", summary="دریافت لیست صنایع با صفحه‌بندی")
async def get_all_sectors(
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("Report.Metadata.Sectors", "ALL")),

        page: int = Query(1, ge=1),
    size: int = Query(10, enum=[10, 50, 100])
):
    try:
        # 🔢 تعداد کل صنایع
        count_result = await db.execute(
            text("SELECT COUNT(DISTINCT sector) FROM daily_joined_data WHERE sector IS NOT NULL")
        )
        total = count_result.scalar_one()

        offset = (page - 1) * size
        result = await db.execute(
            text("""
                SELECT DISTINCT sector
                FROM daily_joined_data
                WHERE sector IS NOT NULL
                ORDER BY sector
                LIMIT :size OFFSET :offset
            """),
            {"size": size, "offset": offset}
        )
        sectors = [row[0] for row in result.fetchall()]

        return create_response(
            status="success",
            message="✅ لیست صنایع با موفقیت دریافت شد",
            data={
                "items": sectors,
                "total": total,
                "page": page,
                "size": size,
                "pages": (total + size - 1) // size
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطا در دریافت صنایع: {str(e)}")


@router.get("/stocks", summary="دریافت نمادهای یک صنعت خاص")
async def get_stocks_in_sector(
        sector: str = Query(..., description="نام صنعت مدنظر برای استخراج نمادها"),
        db: AsyncSession = Depends(get_db),
        _: models.User = Depends( require_permissions ("Report.Metadata.Stocks","ALL"))  # یا پرمیشن مناسب
):
    try:
        result = await db.execute(
            text("""
                SELECT DISTINCT stock_ticker
                FROM daily_joined_data
                WHERE sector = :sector
                ORDER BY stock_ticker
            """),
            {"sector": sector}
        )
        stocks = [row[0] for row in result.fetchall() if row[0] is not None]

        return create_response(data=stocks)

    except Exception as e:
        logger.exception("❌ خطا در دریافت نمادهای صنعت:")
        raise HTTPException(status_code=500, detail="خطا در دریافت لیست نمادها")


@router.get("/sector-stocks", summary="دریافت لیست صنایع با نمادهای مربوطه")
async def get_sector_with_stocks(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.Metadata.SectorStocks","ALL"))
):
    try:
        result = await db.execute(
            text("""
                SELECT DISTINCT sector, stock_ticker
                FROM daily_joined_data
                WHERE sector IS NOT NULL
                ORDER BY sector, stock_ticker
            """)
        )
        rows = result.fetchall()

        df = pd.DataFrame(rows, columns=["sector", "stock_ticker"])
        grouped = df.groupby("sector")["stock_ticker"].apply(list).reset_index()
        result_data = grouped.to_dict(orient="records")

        return create_response(data=result_data)

    except Exception as e:
        logger.exception("❌ خطا در دریافت لیست صنایع و نمادها:")
        raise HTTPException(status_code=500, detail="خطا در دریافت داده‌ها")