from fastapi import APIRouter, Query
from backend.db.connection import get_engine
from fastapi.responses import JSONResponse
import pandas as pd

router = APIRouter()

@router.get("/net-flow")
def get_sankey_combined(
    mode: str = Query("sector", enum=["sector", "intra-sector"]),
    sector: str = Query(None)
):
    try:
        engine = get_engine()
        with engine.connect() as conn:

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
                df = pd.read_sql(query, conn)

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
                    return JSONResponse(content={"error": "sector parameter is required for intra-sector mode"}, status_code=400)

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
                WHERE "Sector" = %(sector)s;
                """
                df = pd.read_sql(query, conn, params={"sector": sector})
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
                    neg = pd.concat([neg, pd.DataFrame([{
                        "Ticker": "Other",
                        "net_real_flow": -diff
                    }])])
                elif abs(total_neg) > total_pos:
                    diff = total_pos + total_neg
                    pos = pd.concat([pos, pd.DataFrame([{
                        "Ticker": "Other",
                        "net_real_flow": diff
                    }])])

                for _, row in pos.iterrows():
                    links.append({
                        "source": "Other",
                        "target": row["Ticker"],
                        "value": abs(row["net_real_flow"])
                    })

                for _, row in neg.iterrows():
                    links.append({
                        "source": row["Ticker"],
                        "target": "Other",
                        "value": abs(row["net_real_flow"])
                    })

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

        return JSONResponse(content=sankey_data)

    except Exception as e:
        print("❌ Error in sankey combined:", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
