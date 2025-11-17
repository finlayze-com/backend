# cron_jobs/daily/capital_increase.py
# -*- coding: utf-8 -*-
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
import os
import psycopg2
from dotenv import load_dotenv

from backend.utils.response import create_response

# تابع جدید در base_updater که فقط چند تیکر خاص را آپدیت می‌کند
from cron_jobs.daily.common.base_updater import run_for_stocks

# Permission
try:
    from backend.users.dependencies import require_permissions
    RequirePerm = lambda: Depends(require_permissions("CapitalIncrease.Run", "ALL"))
except Exception:
    def RequirePerm():
        return None


router = APIRouter(
    prefix="/admin/capital-increase",
    tags=["📈 Capital Increase"],
)


class CapitalIncreaseRequest(BaseModel):
    # توجه: در عمل این همان insCode است
    symboldetail_id: int


def _load_db_url():
    here = os.path.dirname(__file__)
    candidates = [
        os.path.join(here, "../../.env"),
        os.path.join(here, "../.env"),
        os.path.join(here, "../../../.env"),
    ]
    for p in candidates:
        if os.path.exists(p):
            load_dotenv(p)
            break
    db_url = os.getenv("DB_URL_SYNC")
    if not db_url:
        raise RuntimeError("DB_URL_SYNC not set in .env")
    return db_url


@router.post("/run", summary="ثبت افزایش سرمایه: حذف و بازسازی داده‌های یک نماد بر اساس insCode")
def run_capital_increase(
    payload: CapitalIncreaseRequest,
    _ = RequirePerm(),
):
    db_url = _load_db_url()

    inscode_input = str(payload.symboldetail_id)  # در عمل الان با insCode کار می‌کنیم

    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        # 1) از روی insCode → stock_ticker را پیدا کن
        cur.execute(
            """
            SELECT stock_ticker, "insCode"
            FROM symboldetail
            WHERE "insCode" = %s
            """,
            (inscode_input,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail=f'symboldetail با insCode={inscode_input} پیدا نشد',
            )

        stock_ticker, inscode_main = row

        if stock_ticker is None:
            raise HTTPException(
                status_code=400,
                detail=f'برای insCode={inscode_input} مقدار stock_ticker خالی است.',
            )

        # 2) همه‌ی ردیف‌های symboldetail که همین stock_ticker را دارند (همه‌ی insCodeهای مرتبط)
        cur.execute(
            """
            SELECT "insCode"
            FROM symboldetail
            WHERE stock_ticker = %s
            ORDER BY "insCode"
            """,
            (stock_ticker,),
        )
        related_rows = cur.fetchall()
        related_inscodes = [r[0] for r in related_rows]

        print(f"[CapitalIncrease] insCode_input={inscode_input} -> stock_ticker={stock_ticker}")
        print(f"  related insCodes = {related_inscodes}")

        # 3) پاک کردن تمام داده‌های روزانه این تیکر
        cur.execute(
            """
            DELETE FROM daily_stock_data
            WHERE stock_ticker = %s
            """,
            (stock_ticker,),
        )
        deleted_rows = cur.rowcount
        conn.commit()

    # 4) دوباره دانلود و ذخیره‌ی داده‌ها برای این تیکر با منطق run_saham
    run_for_stocks([stock_ticker], "daily_stock_data")

    return create_response(
        message="افزایش سرمایه ثبت شد؛ داده‌های نماد پاک و دوباره بارگذاری شدند.",
        data={
            # اسم فیلد را فعلاً همان symboldetail_id نگه داشتیم ولی در عمل insCode است
            "input_inscode": inscode_input,
            "stock_ticker": stock_ticker,
            "deleted_daily_rows": deleted_rows,
            "related_inscodes": related_inscodes,
        },
        status_code=200,
    )
