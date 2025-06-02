from fastapi import APIRouter, Query
from backend.db.connection import get_engine
from sqlalchemy import text
import pandas as pd
from fastapi.responses import JSONResponse

router = APIRouter()

@router.get("/candlestick/rawdata")
def get_rawdata_for_echarts(
    stock: str = Query(...),
    timeframe: str = Query("daily", enum=["daily", "weekly"]),
    currency: str = Query("rial", enum=["rial", "dollar"])
):
    try:
        engine = get_engine()
        table = "daily_joined_data" if timeframe == "daily" else "weekly_joined_data"
        date_col = "date_miladi" if timeframe == "daily" else "week_end"

        if currency == "rial":
            query = text(f"""
                SELECT {date_col} AS date,
                       open, close, low, high, volume
                FROM {table}
                WHERE stock_ticker = :stock
                ORDER BY {date_col}
            """)
        else:  # dollar
            query = text(f"""
                SELECT {date_col} AS date,
                       adjust_open_usd AS open,
                       adjust_close_usd AS close,
                       adjust_low_usd AS low,
                       adjust_high_usd AS high,
                       value_usd AS volume
                FROM {table}
                WHERE stock_ticker = :stock
                ORDER BY {date_col}
            """)

        df = pd.read_sql(query, engine, params={"stock": stock})
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y/%m/%d")
        df = df.fillna(0)

        rawData = df[["date", "open", "close", "low", "high", "volume"]].values.tolist()
        return rawData

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
