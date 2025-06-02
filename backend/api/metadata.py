from fastapi import APIRouter, Query
from backend.db.connection import get_engine
from sqlalchemy import text
import pandas as pd

router = APIRouter()

@router.get("/metadata/sectors")
def get_all_sectors():
    engine = get_engine()
    query = text("SELECT DISTINCT sector FROM daily_joined_data WHERE sector IS NOT NULL ORDER BY sector")
    df = pd.read_sql(query, engine)
    return df["sector"].dropna().tolist()

@router.get("/metadata/stocks")
def get_stocks_in_sector(sector: str = Query(...)):
    engine = get_engine()
    query = text("""
        SELECT DISTINCT stock_ticker
        FROM daily_joined_data
        WHERE sector = :sector
        ORDER BY stock_ticker
    """)
    df = pd.read_sql(query, engine, params={"sector": sector})
    return df["stock_ticker"].dropna().tolist()


@router.get("/metadata/sector-stocks")
def get_sector_with_stocks():
    engine = get_engine()
    query = text("""
        SELECT DISTINCT sector, stock_ticker
        FROM daily_joined_data
        WHERE sector IS NOT NULL
        ORDER BY sector, stock_ticker
    """)
    df = pd.read_sql(query, engine)

    # گروه‌بندی با پانداس
    grouped = df.groupby("sector")["stock_ticker"].apply(list).reset_index()
    result = grouped.to_dict(orient="records")
    return result
