from fastapi import APIRouter, Query, Depends, HTTPException
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
        ("ي", "ی"),   # ya عربی → ya فارسی
        ("ك", "ک"),   # kaf عربی → kaf فارسی
        ("\u200c", ""),  # نیم‌فاصله (ZWNJ) → حذف
        ("ـ", ""),    # کشیدگی → حذف
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
    _ = Depends(require_permissions("Report.Treemap"))  # ← اگر اسم دقیق پرمیشن فرق دارد همینجا عوضش کن
):
    try:
        sql_name = "treemap_daily" if timeframe == Timeframe.daily else "treemap_weekly"
        sql = load_sql(sql_name)

        result = await db.execute(text(sql))
        rows = result.mappings().all()
        if not rows:
            return create_response(
                status="ok",
                message="داده‌ای برای treemap یافت نشد.",
                data={"items": [], "meta": {"timeframe": timeframe, "count": 0}}
            )

        df = pd.DataFrame(rows)

        # ستون نرمال‌شده سکتور برای مقایسه‌ی امن
        df["sector_norm"] = df["sector"].astype(str).apply(normalize_persian)

        if sector:
            norm_sector = normalize_persian(sector)
            df = df[df["sector_norm"] == norm_sector]

        if not include_etf:
            etf_norm = normalize_persian("صندوق سرمایه گذاری قابل معامله")
            df = df[df["sector_norm"] != etf_norm]


        # پاک‌سازی NaN/Inf
        df = df.replace([float("inf"), float("-inf")], 0).fillna(0)

        # ساختار مناسب ECharts Treemap
        tree = defaultdict(list)
        for _, row in df.iterrows():
            size_val = 1 if size_mode == "equal" else row.get(size_mode, 0)
            node = {
                "name": row["stock_ticker"],
                "value": [
                    round((size_val or 0) / 1e9, 2),         # سایز نود
                    round((row.get("value", 0) or 0) / 1e9, 2),  # ارزش معاملات
                    round((row.get("price_change", 0) or 0), 2)  # برای رنگ
                ]
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
                    "count": int(df.shape[0])
                }
            }
        )

    except Exception as e:
        logger.exception("❌ Error in treemap API")
        # می‌تونی از هندلرهای سراسری هم کمک بگیری؛ اینجا پیام یکپارچه می‌دهیم
        raise HTTPException(status_code=500, detail=create_response(
            status="failed",
            message="خطا در پردازش داده‌های treemap",
            data={"error": str(e)}
        )["message"])
