from fastapi import APIRouter, Query
from backend.db.connection import get_engine
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text
import pandas as pd

router = APIRouter()

@router.get("/real-money-flow/timeseries")
def get_real_money_flow_timeseries(
    timeframe: str = Query("daily", enum=["daily", "weekly"]),
    level: str = Query("sector", enum=["market", "sector", "ticker"]),
    sector: str = Query(None),
    ticker: str = Query(None),
    currency: str = Query("rial", enum=["rial", "dollar"])
):
    try:
        table = "daily_joined_data" if timeframe == "daily" else "weekly_joined_data"
        date_col = "j_date" if timeframe == "daily" else "week_end"

        if currency == "dollar":
            flow_expr = "(buy_i_value - sell_i_value) / NULLIF(dollar_rate, 0)"
        else:
            flow_expr = "(buy_i_value - sell_i_value)"

        if level == "market":
            query = f"""
                SELECT {date_col} AS date,
                       SUM({flow_expr}) AS real_money_flow
                FROM {table}
                GROUP BY {date_col}
                ORDER BY {date_col}
            """
            params = {}

        elif level == "sector":
            query = f"""
                SELECT {date_col} AS date,
                       sector,
                       SUM({flow_expr}) AS real_money_flow
                FROM {table}
                WHERE sector IS NOT NULL
            """
            if sector:
                query += " AND sector = :sector"
                params = {"sector": sector}
            else:
                params = {}
            query += f" GROUP BY {date_col}, sector ORDER BY {date_col}, sector"

        elif level == "ticker":
            if not ticker:
                return JSONResponse(content={"error": "ticker is required for ticker level"}, status_code=400)

            query = f"""
                SELECT {date_col} AS date,
                       stock_ticker,
                       sector,
                       {flow_expr} AS real_money_flow
                FROM {table}
                WHERE stock_ticker = :ticker
                ORDER BY {date_col}
            """
            params = {"ticker": ticker}

        else:
            return JSONResponse(content={"error": "Invalid level"}, status_code=400)

        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)

        df = df.fillna(0)
        return JSONResponse(content=jsonable_encoder(df.to_dict(orient="records")))

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)