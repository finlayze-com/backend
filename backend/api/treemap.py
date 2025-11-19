# -*- coding: utf-8 -*-
from fastapi import APIRouter, Query, Depends
from enum import Enum
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
from collections import defaultdict

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.sql_loader import load_sql
from backend.utils.logger import logger
from backend.utils.response import create_response  # ساختار پاسخ واحد

router = APIRouter(prefix="", tags=["📊 Treemap"])


def normalize_persian(text: str | None):
    """
    نرمال‌سازی حروف عربی/فارسی + حذف نیم‌فاصله و کشیدگی
    """
    if text is None:
        return None
    if not isinstance(text, str):
        text = str(text)

    text = text.strip().lower()
    replacements = [
        ("ي", "ی"),        # ya عربی → ya فارسی
        ("ك", "ک"),        # kaf عربی → kaf فارسی
        ("\u200c", ""),    # نیم‌فاصله (ZWNJ) → حذف
        ("ـ", ""),         # کشیدگی → حذف
    ]
    for src, dst in replacements:
        text = text.replace(src, dst)
    return text


class Timeframe(str, Enum):
    daily = "daily"
    weekly = "weekly"


@router.get("/treemap", summary="داده‌های Treemap بازار (روزانه/هفتگی)")
async def get_treemap_data(
    timeframe: Timeframe = Query(Timeframe.daily, description="تایم‌فریم روزانه یا هفتگی"),
    size_mode: str = Query(
        "marketcap",
        enum=["marketcap", "value", "net_haghighi", "equal"],
        description="معیار سایز: ارزش بازار، ارزش معاملات، خالص ورود حقیقی یا برابر (equal)"
    ),
    sector: str = Query(None, description="فیلتر بر اساس صنعت"),
    include_etf: bool = Query(True, description="آیا ETFها نمایش داده شوند؟"),
    db: AsyncSession = Depends(get_db),
    _ = Depends(require_permissions("Report.Treemap")),
):
    try:
        # انتخاب SQL بر اساس تایم‌فریم
        sql_name = "treemap_daily" if timeframe == Timeframe.daily else "treemap_weekly"
        sql = load_sql(sql_name)

        result = await db.execute(text(sql))
        rows = result.mappings().all()

        if not rows:
            return create_response(
                status="ok",
                message="داده‌ای برای treemap یافت نشد.",
                data={
                    "items": [],
                    "meta": {
                        "timeframe": timeframe,
                        "size_mode": size_mode,
                        "sector_filter": sector,
                        "include_etf": include_etf,
                        "count": 0,
                    },
                },
            )

        df = pd.DataFrame(rows)

        # ستون نرمال‌شده‌ی سکتور برای مقایسه‌ی امن (عربی/فارسی)
        df["sector_norm"] = df["sector"].astype(str).apply(normalize_persian)

        # فیلتر صنعت (در صورت ارسال)
        if sector:
            norm_sector = normalize_persian(sector)
            df = df[df["sector_norm"] == norm_sector]

        # فیلتر حذف ETFها
        if not include_etf:
            etf_norm = normalize_persian("صندوق سرمایه گذاری قابل معامله")
            df = df[df["sector_norm"] != etf_norm]

        # پاک‌سازی NaN / Inf
        df = df.replace([float("inf"), float("-inf")], 0).fillna(0)

        # ساختار مناسب ECharts Treemap
        tree = defaultdict(list)
        for _, row in df.iterrows():
            # 1) مقدار خام سایز بر اساس mode
            if size_mode == "equal":
                size_raw = 1.0
            else:
                size_raw = float(row.get(size_mode) or 0)

            # 2) اسکیل کردن سایز برای نمودار
            if size_mode in ("marketcap", "value", "net_haghighi"):
                # تبدیل به میلیارد (ریال یا پول حقیقی)
                size_for_chart = round(size_raw / 1e10, 3)
            else:
                size_for_chart = round(size_raw, 3)

            # 3) سایر مقادیر
            value_raw = float(row.get("value") or 0)
            price_change = float(row.get("price_change") or 0)

            node = {
                "name": row["stock_ticker"],
                "value": [
                    size_for_chart,                   # سایز نود (بسته به mode)
                    round(value_raw / 1e10, 3),        # ارزش معاملات به میلیارد
                    round(price_change, 3),           # درصد تغییر قیمت برای رنگ
                ],
            }
            tree[row["sector"]].append(node)

        treemap_data = [{"name": s, "children": children} for s, children in tree.items()]

        return create_response(
            status="ok",
            message="داده‌های treemap با موفقیت آماده شد.",
            data={
                "items": treemap_data,
                "meta": {
                    "timeframe": timeframe,
                    "size_mode": size_mode,
                    "sector_filter": sector,
                    "include_etf": include_etf,
                    "count": int(df.shape[0]),
                },
            },
        )

    except Exception as e:
        logger.exception("❌ Error in treemap API")
        # پاسخ یکپارچه در صورت خطا
        return create_response(
            status="failed",
            status_code=500,
            message="خطا در پردازش داده‌های treemap",
            data={"error": str(e)},
        )
