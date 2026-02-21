"""create mv_orderbook_report

Revision ID: 33f4266ac023
Revises: 4296e7cb75c4
Create Date: 2026-02-08 08:19:54.218818
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '33f4266ac023'
down_revision: Union[str, Sequence[str], None] = '4296e7cb75c4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 1) ساخت MV
    op.execute(r"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_orderbook_report AS
    WITH
    /* ----------------------------
      0) ETF mapping از symboldetail (با منطق جدید)
    -----------------------------*/
    sym_etf_raw AS (
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower("stock_ticker")), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        NULLIF(trim("subsector"), '') AS subsector_raw,
        NULLIF(trim("instrument_type"), '') AS instrument_type
      FROM public.symboldetail
      WHERE "sector" = 'صندوق سرمايه گذاري قابل معامله'
        AND "market" <> 'بازار مشتقه'
    ),

    sym_etf_norm AS (
      SELECT
        ticker_key,
        instrument_type,
        regexp_replace(
          regexp_replace(
            replace(replace(replace(trim(lower(COALESCE(subsector_raw, ''))), 'ي','ی'),'ك','ک'), chr(8204), ''),
            '\s*:\s*', ' : ', 'g'
          ),
          '\s+', ' ', 'g'
        ) AS subsector_clean
      FROM sym_etf_raw
    ),

    sym_etf AS (
      SELECT
        ticker_key,
        CASE
          -- ✅ اگر subsector خالی بود ولی instrument_type مشخص بود
          WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '')
               AND instrument_type = 'fund_gold'
            THEN 'طلا'
          WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '')
               AND instrument_type = 'fund_zafran'
            THEN 'زعفران'

          -- ✅ املاک و مستغلات
          WHEN subsector_clean ILIKE '%املاک%' AND subsector_clean ILIKE '%مستغلات%'
            THEN 'املاک و مستغلات'

          -- ✅ سهامی شاخصی (قبل از سهامی)
          WHEN subsector_clean ILIKE '%سهام%' AND subsector_clean ILIKE '%شاخص%'
            THEN 'سهامی شاخصی'
          WHEN subsector_clean ILIKE '%سهامي%' AND subsector_clean ILIKE '%شاخصي%'
            THEN 'سهامی شاخصی'

          WHEN subsector_clean ILIKE '%اهرم%' THEN 'اهرمـی'
          WHEN subsector_clean ILIKE '%طلا%' OR subsector_clean ILIKE '%سکه%' THEN 'طلا'

          -- ✅ بخشی
          WHEN subsector_clean ILIKE '%بخشی%' THEN 'بخشی'

          WHEN subsector_clean ILIKE '%درآمد ثابت%'
            OR subsector_clean ILIKE '%در امد ثابت%'
            OR subsector_clean ILIKE '%در اوراق بهادار با درآمد ثابت%'
            OR subsector_clean ILIKE '%در اوارق بهادار با درآمد ثابت%'
            OR subsector_clean ILIKE '%در اوراق بهادار با%درآمد ثابت%'
          THEN 'درآمد ثابت'

          WHEN subsector_clean ILIKE '%مختلط%' THEN 'مختلط'
          WHEN subsector_clean ILIKE '%کالا%' OR subsector_clean ILIKE '%commodity%' THEN 'کالایی'

          WHEN subsector_clean ILIKE '%سهام%' OR subsector_clean ILIKE '%سهامی%' OR subsector_clean ILIKE '%سهامي%'
            THEN 'سهامی'

          ELSE 'other'
        END AS subsector_norm
      FROM sym_etf_norm
      GROUP BY 1,2
    ),

    /* ----------------------------
      1) پیدا کردن آخرین روز "واقعاً فعال"
    -----------------------------*/
    valid_days AS (
      SELECT
        ("Timestamp"::date) AS d,
        COUNT(*) AS rows_cnt,
        COUNT(DISTINCT "Symbol") AS symbols_cnt
      FROM public.orderbook_snapshot
      GROUP BY 1
      HAVING COUNT(DISTINCT "Symbol") >= 100
    ),
    target_day AS (
      SELECT MAX(d) AS d
      FROM valid_days
    ),

    /* ----------------------------
      2) آماده‌سازی داده + bucket دقیقه‌ای + ticker_key
    -----------------------------*/
    ob0 AS (
      SELECT
        o.*,
        COALESCE(NULLIF(trim(o."Sector"), ''), 'other') AS sector,
        date_trunc('minute', o."Timestamp") AS bucket_minute,

        regexp_replace(
          replace(replace(replace(trim(lower(o."Symbol")), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key
      FROM public.orderbook_snapshot o
      JOIN target_day td
        ON o."Timestamp"::date = td.d
    ),

    /* ----------------------------
      3) تعیین ساعت پایان برای هر sector
    -----------------------------*/
    ob1 AS (
      SELECT
        o0.*,
        se.subsector_norm AS etf_subsector,
        CASE
          WHEN se.subsector_norm IS NOT NULL OR o0.sector ILIKE '%صندوق%' THEN time '18:00'
          ELSE time '12:30'
        END AS session_end
      FROM ob0 o0
      LEFT JOIN sym_etf se
        ON se.ticker_key = o0.ticker_key
    ),

    /* ----------------------------
      4) ساخت key نهایی
    -----------------------------*/
    ob2 AS (
      SELECT
        *,
        CASE
          WHEN etf_subsector IS NOT NULL
            THEN 'صندوق سرمایه گذاری قابل معامله | ' || etf_subsector
          ELSE sector
        END AS key
      FROM ob1
    ),

    /* ----------------------------
      5) آخرین minute معتبر برای هر key
    -----------------------------*/
    latest_bucket_per_key AS (
      SELECT
        key,
        MAX(bucket_minute) AS last_bucket
      FROM ob2
      WHERE (bucket_minute::time) <= session_end
      GROUP BY key
    ),

    /* ----------------------------
      6) گرفتن همه رکوردهای همان minute برای هر key
    -----------------------------*/
    ob_pick AS (
      SELECT
        o.key,
        o.bucket_minute AS ts,
        (SELECT d FROM target_day) AS snapshot_day,
        o."Symbol",

        (COALESCE(o."BuyPrice1",0)::numeric  * COALESCE(o."BuyVolume1",0)::numeric)  AS buy_v1,
        (COALESCE(o."BuyPrice2",0)::numeric  * COALESCE(o."BuyVolume2",0)::numeric)  AS buy_v2,
        (COALESCE(o."BuyPrice3",0)::numeric  * COALESCE(o."BuyVolume3",0)::numeric)  AS buy_v3,
        (COALESCE(o."BuyPrice4",0)::numeric  * COALESCE(o."BuyVolume4",0)::numeric)  AS buy_v4,
        (COALESCE(o."BuyPrice5",0)::numeric  * COALESCE(o."BuyVolume5",0)::numeric)  AS buy_v5,

        (COALESCE(o."SellPrice1",0)::numeric * COALESCE(o."SellVolume1",0)::numeric) AS sell_v1,
        (COALESCE(o."SellPrice2",0)::numeric * COALESCE(o."SellVolume2",0)::numeric) AS sell_v2,
        (COALESCE(o."SellPrice3",0)::numeric * COALESCE(o."SellVolume3",0)::numeric) AS sell_v3,
        (COALESCE(o."SellPrice4",0)::numeric * COALESCE(o."SellVolume4",0)::numeric) AS sell_v4,
        (COALESCE(o."SellPrice5",0)::numeric * COALESCE(o."SellVolume5",0)::numeric) AS sell_v5,

        COALESCE(o."BuyPrice1",0)::numeric  AS best_bid,
        COALESCE(o."SellPrice1",0)::numeric AS best_ask
      FROM ob2 o
      JOIN latest_bucket_per_key l
        ON o.key = l.key
       AND o.bucket_minute = l.last_bucket
    ),

    /* ----------------------------
      7) گزارش key
    -----------------------------*/
    key_report AS (
      SELECT
        key AS sector,
        MAX(snapshot_day) AS snapshot_day,
        MAX(ts) AS ts,
        COUNT(DISTINCT "Symbol") AS symbols_count,

        SUM(buy_v1)  AS buy_value1,
        SUM(sell_v1) AS sell_value1,

        SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5) AS buy_value5,
        SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5) AS sell_value5,

        (SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5) - SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5)) AS net_order_value,
        (SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5) + SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5)) AS orderbook_total_value,

        (SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5) - SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5))
        / NULLIF(
            SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5) + SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5),
            0
          ) AS imbalance5,

        SUM(buy_v1) / NULLIF(SUM(sell_v1),0) AS bidask_ratio1,

        SUM(buy_v1)  / NULLIF(SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5),0) AS buy_concentration1,
        SUM(sell_v1) / NULLIF(SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5),0) AS sell_concentration1,

        AVG(
          CASE
            WHEN best_bid > 0 AND best_ask > 0
              THEN (best_ask - best_bid) / ((best_ask + best_bid)/2.0)
            ELSE NULL
          END
        ) AS spread_pct_avg
      FROM ob_pick
      GROUP BY key
    ),

    /* ----------------------------
      8) گزارش کل بازار
    -----------------------------*/
    market_report AS (
      SELECT
        '__ALL__'::text AS sector,
        MAX(snapshot_day) AS snapshot_day,
        MAX(ts) AS ts,
        COUNT(DISTINCT "Symbol") AS symbols_count,

        SUM(buy_v1)  AS buy_value1,
        SUM(sell_v1) AS sell_value1,

        SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5) AS buy_value5,
        SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5) AS sell_value5,

        (SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5) - SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5)) AS net_order_value,
        (SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5) + SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5)) AS orderbook_total_value,

        (SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5) - SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5))
        / NULLIF(
            SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5) + SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5),
            0
          ) AS imbalance5,

        SUM(buy_v1) / NULLIF(SUM(sell_v1),0) AS bidask_ratio1,

        SUM(buy_v1)  / NULLIF(SUM(buy_v1+buy_v2+buy_v3+buy_v4+buy_v5),0) AS buy_concentration1,
        SUM(sell_v1) / NULLIF(SUM(sell_v1+sell_v2+sell_v3+sell_v4+sell_v5),0) AS sell_concentration1,

        AVG(
          CASE
            WHEN best_bid > 0 AND best_ask > 0
              THEN (best_ask - best_bid) / ((best_ask + best_bid)/2.0)
            ELSE NULL
          END
        ) AS spread_pct_avg
      FROM ob_pick
    )

    SELECT
      sector,
      snapshot_day,
      ts,
      symbols_count,
      buy_value1,
      sell_value1,
      buy_value5,
      sell_value5,
      net_order_value,
      orderbook_total_value,
      imbalance5,
      CASE
        WHEN imbalance5 > 0.15 THEN 'bullish'
        WHEN imbalance5 < -0.15 THEN 'bearish'
        ELSE 'neutral'
      END AS imbalance_state,
      bidask_ratio1,
      buy_concentration1,
      sell_concentration1,
      spread_pct_avg
    FROM (
      SELECT * FROM market_report
      UNION ALL
      SELECT * FROM key_report
    ) x;
    """)

    # 2) ایندکس‌های MV
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_orderbook_report_sector
    ON public.mv_orderbook_report (sector);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_orderbook_report_ts
    ON public.mv_orderbook_report (ts DESC);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_orderbook_report_buy_value5
    ON public.mv_orderbook_report (buy_value5 DESC);
    """)

    # 3) ایندکس‌های پیشنهادی روی جدول خام
    op.execute("""
    CREATE INDEX IF NOT EXISTS brin_orderbook_snapshot_ts
    ON public.orderbook_snapshot
    USING brin ("Timestamp");
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_orderbook_snapshot_ts_date
    ON public.orderbook_snapshot (("Timestamp"::date));
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_orderbook_snapshot_day_symbol
    ON public.orderbook_snapshot (("Timestamp"::date), "Symbol");
    """)


def downgrade():
    op.execute("DROP INDEX IF EXISTS public.ux_mv_orderbook_report_sector;")
    op.execute("DROP INDEX IF EXISTS public.ix_mv_orderbook_report_ts;")
    op.execute("DROP INDEX IF EXISTS public.ix_mv_orderbook_report_buy_value5;")

    op.execute("DROP INDEX IF EXISTS public.brin_orderbook_snapshot_ts;")
    op.execute("DROP INDEX IF EXISTS public.ix_orderbook_snapshot_ts_date;")
    op.execute("DROP INDEX IF EXISTS public.ix_orderbook_snapshot_day_symbol;")

    op.execute("DROP MATERIALIZED VIEW IF EXISTS public.mv_orderbook_report;")
