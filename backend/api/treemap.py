from fastapi import APIRouter, Query
from enum import Enum
from backend.db.connection import get_engine
from sqlalchemy import text
import pandas as pd
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from backend.utils.sql_loader import load_sql
from collections import defaultdict

router = APIRouter()

# 🔹 Enum برای تایم‌فریم انتخابی
class Timeframe(str, Enum):
    daily = "daily"
    weekly = "weekly"

@router.get("/treemap")
def get_treemap_data(
    timeframe: Timeframe = Query(Timeframe.daily, description="تایم‌فریم روزانه یا هفتگی"),
    size_mode: str = Query(
        "marketcap",
        enum=["marketcap", "value", "net_haghighi", "equal"],
        description="معیار سایز: ارزش بازار، ارزش معاملات، خالص ورود حقیقی یا برابر (equal)"
    ),
    sector: str = Query(None, description="فیلتر بر اساس صنعت"),
    include_etf: bool = Query(True, description="آیا ETFها نمایش داده شوند؟")
):
    try:
        engine = get_engine()
        sql = load_sql("treemap_daily") if timeframe == Timeframe.daily else load_sql("treemap_weekly")

        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)

        if sector:
            df = df[df["sector"] == sector]
        if not include_etf:
            df = df[df["sector"] != "صندوق سرمایه گذاری قابل معامله"]

        df = df.replace([float("inf"), float("-inf")], 0).fillna(0)

        # ساختار مناسب ECharts Treemap
        tree = defaultdict(list)
        for _, row in df.iterrows():
            size = 1 if size_mode == "equal" else row.get(size_mode, 0)
            node = {
                "name": row["stock_ticker"],
                "value": [
                    round(size / 1e9, 2),                 # سایز نود
                    round(row.get("value", 0) / 1e9, 2),  # ارزش معاملات
                    round(row.get("price_change", 0), 2)  # برای رنگ
                ]
            }
            tree[row["sector"]].append(node)

        treemap_data = [{"name": sector, "children": children} for sector, children in tree.items()]
        return JSONResponse(content=jsonable_encoder(treemap_data))

    except Exception as e:
        print("❌ Error in treemap API:", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
