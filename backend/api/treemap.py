# ✅ فایل: backend/api/treemap.py

from fastapi import APIRouter, Query
from backend.db.connection import get_engine
from sqlalchemy import text
import pandas as pd
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from backend.utils.sql_loader import load_sql

router = APIRouter()

@router.get("/treemap/{timeframe}")
def get_treemap_data(
    timeframe: str,
    size_mode: str = Query("market_cap"),
    sector: str = Query(None),
    include_etf: bool = Query(True)
):
    try:
        engine = get_engine()
        sql = load_sql("treemap_daily") if timeframe == "daily" else load_sql("treemap_weekly")
        print("📥 Loaded SQL:", sql[:100])  # نمایش بخشی از SQL

        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)

        if sector:
            df = df[df["sector"] == sector]
        if not include_etf:
            df = df[df["sector"] != "صندوق سرمایه گذاری قابل معامله"]

        df["size"] = df.get(size_mode, 0)
        df = df.replace([float("inf"), float("-inf")], 0)
        df = df.fillna(0)
        print("🔍 Sample data:\n", df[["stock_ticker", "sector", "size"]].head())

        return JSONResponse(content=jsonable_encoder(df.to_dict(orient="records")))

    except Exception as e:
        print("❌ Error in treemap API:", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
