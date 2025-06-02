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
            group_col = "sector"
        elif mode == "intra-sector":
            if not sector:
                return JSONResponse(content={"error": "sector is required in intra-sector mode"}, status_code=400)
            sql = load_sql("orderbook_intrasector_timeseries")
            params = {"sector": sector}
            group_col = "Symbol"
        else:
            return JSONResponse(content={"error": "invalid mode"}, status_code=400)

        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)

        df["net_value"] = df["total_buy"] - df["total_sell"]
        df = df.fillna(0)

        if "minute" not in df.columns or group_col not in df.columns:
            return JSONResponse(content={"error": "Missing required columns in result"}, status_code=500)

        # فقط ستون‌های مورد نیاز برای ECharts Line Chart
        df_out = df[["minute", group_col, "net_value"]].copy()
        df_out = df_out.rename(columns={group_col: "name"})  # برای یکنواختی

        return JSONResponse(content=jsonable_encoder(df_out.to_dict(orient="records")))

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
