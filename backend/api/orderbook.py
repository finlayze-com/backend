# ✅ فایل: backend/api/orderbook.py (با پشتیبانی از حالت بین‌صنعتی و درون‌صنعتی)

from fastapi import APIRouter, Query
from backend.db.connection import get_engine
from backend.utils.sql_loader import load_sql
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text
import pandas as pd

router = APIRouter()

@router.get("/orderbook/timeseries")
def get_orderbook_timeseries(
    mode: str = Query("sector", enum=["sector", "intra-sector"]),
    sector: str = Query(None)
):
    try:
        engine = get_engine()

        if mode == "sector":
            sql = load_sql("orderbook_sector_timeseries")
            params = {}
        elif mode == "intra-sector":
            if not sector:
                return JSONResponse(content={"error": "sector is required in intra-sector mode"}, status_code=400)
            sql = load_sql("orderbook_intrasector_timeseries")
            params = {"sector": sector}
        else:
            return JSONResponse(content={"error": "invalid mode"}, status_code=400)

        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)

        df["net_value"] = df["total_buy"] - df["total_sell"]
        df = df.fillna(0)

        return JSONResponse(content=jsonable_encoder(df.to_dict(orient="records")))

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
