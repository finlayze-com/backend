from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.response import create_response
from backend.utils.logger import logger
import pandas as pd

router = APIRouter(
    prefix="",
    tags=["📊 Sankey"]
)

@router.get("/net-flow", summary="جریان پول حقیقی در سطح صنعت یا درون‌صنعت")
async def get_sankey_combined(
    mode: str = Query("sector", enum=["sector", "intra-sector"]),
    sector: str = Query(None, description="نام صنعت (فقط برای intra-sector نیاز است)"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.Sankey"))
):
    try:
        if mode == "sector":
            query = """
                WITH filtered AS (
                    SELECT *
                    FROM live_market_data
                    WHERE "Vol_Buy_R" IS NOT NULL AND "Vol_Sell_R" IS NOT NULL AND "Close" IS NOT NULL
                ),
                latest_rows AS (
                    SELECT * FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (PARTITION BY "Ticker" ORDER BY "updated_at" DESC) AS rn
                        FROM filtered
                    ) sub WHERE rn = 1
                )
                SELECT "Sector",
                       SUM(("Vol_Buy_R" - "Vol_Sell_R") * "Close") AS net_real_flow
                FROM latest_rows
                GROUP BY "Sector"
                ORDER BY net_real_flow DESC;
            """
            result = await db.execute(text(query))
            rows =  result.all()  # ← اینجا await یادت نره
            df = pd.DataFrame(rows, columns=["Sector", "net_real_flow"])

            links = []
            node_names = set()

            for _, row in df.iterrows():
                sector = row["Sector"]
                flow = row["net_real_flow"]
                if flow > 0:
                    links.append({"source": "Other", "target": sector, "value": abs(flow)})
                elif flow < 0:
                    links.append({"source": sector, "target": "Other", "value": abs(flow)})
                node_names.add(sector)

            node_names.add("Other")
            nodes = [{"name": name} for name in node_names]

        elif mode == "intra-sector":
            if not sector:
                raise HTTPException(status_code=400, detail="پارامتر sector الزامی است.")

            query = """
                WITH filtered AS (
                    SELECT *
                    FROM live_market_data
                    WHERE "Vol_Buy_R" IS NOT NULL AND "Vol_Sell_R" IS NOT NULL AND "Close" IS NOT NULL
                ),
                latest_rows AS (
                    SELECT * FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (PARTITION BY "Ticker" ORDER BY "updated_at" DESC) AS rn
                        FROM filtered
                    ) sub WHERE rn = 1
                )
                SELECT "Ticker", "Vol_Buy_R", "Vol_Sell_R", "Close"
                FROM latest_rows
                WHERE "Sector" = :sector;
            """
            result = await db.execute(text(query), {"sector": sector})
            df = pd.DataFrame(result.fetchall(), columns=["Ticker", "Vol_Buy_R", "Vol_Sell_R", "Close"])
            df["net_real_flow"] = (df["Vol_Buy_R"] - df["Vol_Sell_R"]) * df["Close"]
            df = df.sort_values("net_real_flow", ascending=False)

            links = []
            nodes = [{"name": name} for name in df["Ticker"]]
            pos = df[df["net_real_flow"] > 0]
            neg = df[df["net_real_flow"] < 0]

            total_pos = pos["net_real_flow"].sum()
            total_neg = neg["net_real_flow"].sum()

            if total_pos > abs(total_neg):
                diff = total_pos + total_neg
                neg = pd.concat([neg, pd.DataFrame([{"Ticker": "Other", "net_real_flow": -diff}])])
            elif abs(total_neg) > total_pos:
                diff = total_pos + total_neg
                pos = pd.concat([pos, pd.DataFrame([{"Ticker": "Other", "net_real_flow": diff}])])

            for _, row in pos.iterrows():
                links.append({"source": "Other", "target": row["Ticker"], "value": abs(row["net_real_flow"])})

            for _, row in neg.iterrows():
                links.append({"source": row["Ticker"], "target": "Other", "value": abs(row["net_real_flow"])})

            if "Other" not in [n["name"] for n in nodes]:
                nodes.append({"name": "Other"})

        sankey_data = {
            "series": {
                "type": "sankey",
                "layout": "none",
                "emphasis": {"focus": "adjacency"},
                "data": nodes,
                "links": links
            }
        }

        return create_response(data=sankey_data)

    except Exception as e:
        logger.exception("❌ خطا در دریافت نمودار سانکی:")
        raise HTTPException(status_code=500, detail="خطا در دریافت داده سانکی")
