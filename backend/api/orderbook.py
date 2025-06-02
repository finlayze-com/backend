from fastapi import APIRouter, Query
from backend.db.connection import get_engine
from backend.utils.sql_loader import load_sql
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text
import pandas as pd
from collections import defaultdict

router = APIRouter()

@router.get("/orderbook/bumpchart")
def get_orderbook_bumpchart_data(
    mode: str = Query("sector", enum=["sector", "intra-sector"]),
    sector: str = Query(None)
):
    try:
        engine = get_engine()

        # تعیین SQL مناسب
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

        # اجرای کوئری
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)

        # محاسبه خالص سفارش
        df["net_value"] = df["total_buy"] - df["total_sell"]
        df = df.fillna(0)

        # بررسی ستون‌های مورد نیاز
        if "minute" not in df.columns or group_col not in df.columns:
            return JSONResponse(content={"error": "Missing required columns in query result"}, status_code=500)

        # استخراج لحظات و گروه‌ها
        minutes = sorted(df["minute"].unique())
        groups = df[group_col].unique().tolist()

        # ساخت دیکشنری رتبه‌ها
        bump_data = defaultdict(list)

        for minute in minutes:
            temp_df = df[df["minute"] == minute].copy()
            temp_df = temp_df.groupby(group_col)["net_value"].sum().reset_index()
            temp_df = temp_df.sort_values(by="net_value", ascending=False).reset_index(drop=True)
            temp_df["rank"] = temp_df.index + 1

            for name in groups:
                rank = temp_df[temp_df[group_col] == name]["rank"].values
                if len(rank) > 0:
                    bump_data[name].append(int(rank[0]))
                else:
                    bump_data[name].append(None)

        # 🔁 جایگزینی مقادیر None با آخرین مقدار معتبر (ffill)
        ranking_df = pd.DataFrame(bump_data, index=minutes).ffill().bfill()
        bump_data_filled = ranking_df.to_dict(orient="list")

        return JSONResponse(content=jsonable_encoder(bump_data_filled))

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
