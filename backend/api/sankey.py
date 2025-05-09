from fastapi import APIRouter
from backend.db.connection import get_engine
import pandas as pd
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

print("📦 [SANKEY] sankey.py is imported")

router = APIRouter()

@router.get("/sector")
def test_sankey_debug():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            query = """
            WITH filtered AS (
                SELECT *
                FROM live_market_data
                WHERE "Vol_Buy_R" IS NOT NULL AND "Vol_Sell_R" IS NOT NULL AND "Close" IS NOT NULL
            ),
            latest_rows AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY "Ticker" ORDER BY "updated_at" DESC) AS rn
                FROM filtered
            )
            SELECT "Sector",
                   SUM(("Vol_Buy_R" - "Vol_Sell_R") * "Close") AS net_real_flow
            FROM latest_rows
            WHERE rn = 1
            GROUP BY "Sector"
            ORDER BY net_real_flow DESC;
            """
            df = pd.read_sql(query, conn)
            print("✅ DataFrame length:", len(df))
            return JSONResponse(content=jsonable_encoder(df.to_dict(orient="records")))
    except Exception as e:
        print("❌ Error:", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
