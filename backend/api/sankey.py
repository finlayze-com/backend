# -*- coding: utf-8 -*-
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.response import create_response
from backend.utils.logger import logger
import pandas as pd

router = APIRouter(prefix="", tags=["📊 Sankey"])

@router.get("/net-flow", summary="جریان پول حقیقی در سطح صنعت یا درون‌صنعت")
async def get_sankey_combined(
    mode: str = Query("sector", enum=["sector", "intra-sector"]),
    sector: str | None = Query(None, description="نام صنعت (فقط برای intra-sector نیاز است)"),
    # کنترل شلوغی خروجی در حالت درون‌صنعت
    top_k: int = Query(30, ge=0, description="تعداد بیشینه‌ی نودها بر اساس |flow|"),
    min_abs_flow: float = Query(0, ge=0, description="کمینه‌ی قدر مطلق جریان برای نمایش"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.Sankey"))
):
    try:
        if mode == "sector":
            # [CHANGED] ستون‌های صریح + cast به numeric برای جلوگیری از overflow
            # query = """
                # WITH filtered AS (
                #     SELECT
                #         "Ticker","Sector","Vol_Buy_R","Vol_Sell_R","Close","updated_at"
                #     FROM live_market_data
                #     WHERE "Vol_Buy_R" IS NOT NULL
                #       AND "Vol_Sell_R" IS NOT NULL
                #       AND "Close"     IS NOT NULL
                # ),
                # latest_rows AS (
                #     SELECT * FROM (
                #         SELECT *,
                #                ROW_NUMBER() OVER (PARTITION BY "Ticker" ORDER BY "updated_at" DESC) AS rn
                #         FROM filtered
                #     ) s WHERE rn = 1
                # )
                # SELECT
                #     "Sector",
                #     SUM( (("Vol_Buy_R" - "Vol_Sell_R")::numeric) * ("Close"::numeric) ) AS net_real_flow
                # FROM latest_rows
                # GROUP BY "Sector"
                # ORDER BY net_real_flow DESC;
            # """
            query = """
                WITH last_day AS (
                    SELECT MAX("updated_at"::date) AS d
                    FROM live_market_data
                    WHERE "Vol_Buy_R" IS NOT NULL
                      AND "Vol_Sell_R" IS NOT NULL
                      AND "Close"     IS NOT NULL
                ),
                latest_rows AS (
                    SELECT DISTINCT ON ("Ticker")
                        "Ticker",
                        "Sector",
                        "Vol_Buy_R",
                        "Vol_Sell_R",
                        "Close"
                    FROM live_market_data, last_day
                    WHERE "Vol_Buy_R" IS NOT NULL
                      AND "Vol_Sell_R" IS NOT NULL
                      AND "Close"     IS NOT NULL
                      AND "updated_at"::date = last_day.d
                    ORDER BY "Ticker", "updated_at" DESC
                )
                SELECT
                    "Sector",
                    SUM( (("Vol_Buy_R" - "Vol_Sell_R")::numeric) * ("Close"::numeric) ) AS net_real_flow
                FROM latest_rows
                GROUP BY "Sector"
                ORDER BY net_real_flow DESC;
            """

            result = await db.execute(text(query))
            rows = result.all()
            df = pd.DataFrame(rows, columns=["Sector", "net_real_flow"])

            logger.info(f"[sector] rows={len(df)} nonzero={(df['net_real_flow']!=0).sum()}")

            if df.empty:
                return create_response(
                    data=None, status_code=204, message="هیچ داده‌ای برای سطح صنعت یافت نشد."
                )

            links, node_names = [], set()
            for _, row in df.iterrows():
                sector_name = row["Sector"]
                flow = float(row["net_real_flow"])
                if flow > 0:
                    links.append({"source": "Other", "target": sector_name, "value": abs(flow)})
                elif flow < 0:
                    links.append({"source": sector_name, "target": "Other", "value": abs(flow)})
                node_names.add(sector_name)

            node_names.add("Other")
            nodes = [{"name": n} for n in node_names]

        # else:  # mode == "intra-sector"
        #     if not sector:
        #         raise HTTPException(status_code=400, detail="پارامتر sector الزامی است.")

            # [CHANGED] فیلتر سکتور مقاوم به فاصله/حروف + محاسبه‌ی امن flow
            # query = """
            #     WITH filtered AS (
            #         SELECT
            #             "Ticker","Sector","Vol_Buy_R","Vol_Sell_R","Close","updated_at"
            #         FROM live_market_data
            #         WHERE "Vol_Buy_R" IS NOT NULL
            #           AND "Vol_Sell_R" IS NOT NULL
            #           AND "Close"     IS NOT NULL
            #           AND trim(both FROM lower("Sector")) = trim(both FROM lower(:sector))
            #     ),
            #     latest_rows AS (
            #         SELECT * FROM (
            #             SELECT *,
            #                    ROW_NUMBER() OVER (PARTITION BY "Ticker" ORDER BY "updated_at" DESC) AS rn
            #             FROM filtered
            #         ) s WHERE rn = 1
            #     )
            #     SELECT
            #         "Ticker",
            #         (("Vol_Buy_R" - "Vol_Sell_R")::numeric) * ("Close"::numeric) AS net_real_flow
            #     FROM latest_rows;
            # """

            # result = await db.execute(text(query), {"sector": sector})
            # rows = result.all()
            # df = pd.DataFrame(rows, columns=["Ticker", "net_real_flow"])
            #
            # logger.info(f"[intra-sector] sector={sector} rows={len(df)} "
            #             f"nonzero={(df['net_real_flow']!=0).sum()}")
            # mode == "intra-sector"

        else:
                if not sector:
                    raise HTTPException(status_code=400, detail="پارامتر sector الزامی است.")

            # آخرین روزی که برای این سکتور دیتا داریم + آخرین ردیف هر نماد در آن روز
                query_intra = """
                    WITH last_day AS (
                        SELECT MAX("updated_at"::date) AS d
                        FROM live_market_data
                        WHERE "Vol_Buy_R" IS NOT NULL
                          AND "Vol_Sell_R" IS NOT NULL
                          AND "Close"     IS NOT NULL
                          AND trim(both FROM lower("Sector")) = trim(both FROM lower(:sector))
                    ),
                    latest_rows AS (
                        SELECT DISTINCT ON ("Ticker")
                            "Ticker",
                            "Sector",
                            "Vol_Buy_R",
                            "Vol_Sell_R",
                            "Close"
                        FROM live_market_data, last_day
                        WHERE "Vol_Buy_R" IS NOT NULL
                          AND "Vol_Sell_R" IS NOT NULL
                          AND "Close"     IS NOT NULL
                          AND trim(both FROM lower("Sector")) = trim(both FROM lower(:sector))
                          AND "updated_at"::date = last_day.d
                        ORDER BY "Ticker", "updated_at" DESC
                    )
                    SELECT
                        "Ticker",
                        (("Vol_Buy_R" - "Vol_Sell_R")::numeric) * ("Close"::numeric) AS net_real_flow
                    FROM latest_rows;
                """

                result = await db.execute(text(query_intra), {"sector": sector})
                rows = result.all()
                df = pd.DataFrame(rows, columns=["Ticker", "net_real_flow"])

                logger.info(
                        f"[intra-sector] sector={sector} rows={len(df)} "
                        f"nonzero={(df['net_real_flow'] != 0).sum() if not df.empty else 0}"
                    )

                if df.empty:
                    return create_response(
                        data=None, status_code=204,
                        message=f"برای سکتور «{sector}» پس از فیلتر نال/آخرین ردیف، داده‌ای موجود نیست."
                    )

                # آستانه‌ها و مرتب‌سازی بر اساس |flow|
                if min_abs_flow > 0:
                    df = df[df["net_real_flow"].abs() >= float(min_abs_flow)]
                if top_k and top_k > 0:
                    df = df.reindex(df["net_real_flow"].abs().sort_values(ascending=False).index).head(top_k)
                if df.empty:
                    return create_response(
                        data=None, status_code=204,
                        message=f"همه‌ی جریان‌ها با فیلترها حذف شدند (top_k={top_k}, min_abs_flow={min_abs_flow})."
                    )

                # [CHANGED] بدون self-loop/صفر؛ فقط لینک‌های معتبر
                df["net_real_flow"] = df["net_real_flow"].astype(float)
                nodes = [{"name": "Other"}] + [{"name": t} for t in df["Ticker"].tolist()]
                # یکتا کردن نودها
                seen, uniq_nodes = set(), []
                for n in nodes:
                    if n["name"] not in seen:
                        uniq_nodes.append(n); seen.add(n["name"])
                nodes = uniq_nodes

                links = []
                pos = df[df["net_real_flow"] > 0]
                neg = df[df["net_real_flow"] < 0]
                for _, r in pos.iterrows():
                    links.append({"source": "Other", "target": r["Ticker"], "value": float(r["net_real_flow"])})
                for _, r in neg.iterrows():
                    links.append({"source": r["Ticker"], "target": "Other", "value": float(abs(r["net_real_flow"]))})

        # شیء ECharts Sankey
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

    except Exception:
        logger.exception("❌ خطا در دریافت نمودار سانکی:")
        raise HTTPException(status_code=500, detail="خطا در دریافت داده سانکی")
