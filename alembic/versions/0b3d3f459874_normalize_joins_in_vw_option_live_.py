"""normalize joins in vw_option_live_enriched

Revision ID: 0b3d3f459874
Revises: 3a40915215e2
Create Date: 2026-03-21 12:58:23.484912

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0b3d3f459874'
down_revision: Union[str, Sequence[str], None] = '3a40915215e2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

"""normalize joins in vw_option_live_enriched

Revision ID: normalize_vw_option_live_enriched_joins
Revises: create_vw_option_live_enriched
Create Date: 2026-03-21
"""

def upgrade():
    op.execute("""
    CREATE OR REPLACE VIEW public.vw_option_live_enriched AS
    WITH latest_live AS (
        SELECT DISTINCT ON ("Ticker")
            "Ticker",
            "Time",
            "Open",
            "High",
            "Low",
            "Close",
            "Final",
            "Close(%)",
            "Final(%)",
            "Value",
            "Volume",
            "updated_at"
        FROM public.live_market_data
        ORDER BY "Ticker", "updated_at" DESC
    ),
    latest_ob AS (
        SELECT DISTINCT ON ("Symbol")
            "insCode",
            "Symbol",
            "Timestamp",
            "BuyPrice1",
            "BuyVolume1",
            "SellPrice1",
            "SellVolume1",
            "BuyPrice2",
            "BuyVolume2",
            "SellPrice2",
            "SellVolume2",
            "BuyPrice3",
            "BuyVolume3",
            "SellPrice3",
            "SellVolume3",
            "BuyPrice4",
            "BuyVolume4",
            "SellPrice4",
            "SellVolume4",
            "BuyPrice5",
            "BuyVolume5",
            "SellPrice5",
            "SellVolume5",
            "Sector"
        FROM public.orderbook_snapshot
        ORDER BY "Symbol", "Timestamp" DESC
    )
    SELECT
        od.id,
        od.name AS option_name,
        od.name_en AS option_name_en,
        od.sector AS option_sector,
        od.sector_code AS option_sector_code,
        od.stock_ticker AS option_ticker,
        od.ins_code,
        od.instrument_id,
        od.option_type,
        od.is_call,
        od.is_put,

        od.underlying_name,
        od.underlying_ticker,

        od.contract_size,
        od.strike_price,
        od.begin_date,
        od.end_date,
        od.days_to_expiry,
        od.is_active,

        -- option live
        opt_live."updated_at" AS option_live_updated_at,
        opt_live."Time" AS option_trade_time,
        opt_live."Open" AS option_open_price,
        opt_live."High" AS option_high_price,
        opt_live."Low" AS option_low_price,
        opt_live."Close" AS option_close_price,
        opt_live."Final" AS option_last_price,
        opt_live."Close(%)" AS option_close_pct,
        opt_live."Final(%)" AS option_final_pct,
        opt_live."Value" AS option_value,
        opt_live."Volume" AS option_volume,

        -- option orderbook
        opt_ob."Timestamp" AS option_orderbook_ts,
        opt_ob."BuyPrice1" AS option_bid_price,
        opt_ob."BuyVolume1" AS option_bid_volume,
        opt_ob."SellPrice1" AS option_ask_price,
        opt_ob."SellVolume1" AS option_ask_volume,
        opt_ob."BuyPrice2" AS option_bid_price_2,
        opt_ob."BuyVolume2" AS option_bid_volume_2,
        opt_ob."SellPrice2" AS option_ask_price_2,
        opt_ob."SellVolume2" AS option_ask_volume_2,

        -- underlying live
        und_live."updated_at" AS underlying_live_updated_at,
        und_live."Time" AS underlying_trade_time,
        und_live."Open" AS spot_open_price,
        und_live."High" AS spot_high_price,
        und_live."Low" AS spot_low_price,
        und_live."Close" AS spot_close_price,
        und_live."Final" AS spot_price,
        und_live."Close(%)" AS spot_close_pct,
        und_live."Final(%)" AS spot_final_pct,
        und_live."Value" AS spot_value,
        und_live."Volume" AS spot_volume,

        -- underlying orderbook
        und_ob."Timestamp" AS underlying_orderbook_ts,
        und_ob."BuyPrice1" AS spot_bid_price,
        und_ob."BuyVolume1" AS spot_bid_volume,
        und_ob."SellPrice1" AS spot_ask_price,
        und_ob."SellVolume1" AS spot_ask_volume,

        -- calculations
        CASE
            WHEN opt_ob."BuyPrice1" IS NOT NULL
             AND opt_ob."SellPrice1" IS NOT NULL
             AND (opt_ob."BuyPrice1" + opt_ob."SellPrice1") > 0
            THEN (opt_ob."SellPrice1" - opt_ob."BuyPrice1")
                 / ((opt_ob."SellPrice1" + opt_ob."BuyPrice1") / 2.0)
            ELSE NULL
        END AS option_spread_pct,

        CASE
            WHEN und_live."Final" IS NOT NULL
             AND od.strike_price IS NOT NULL
            THEN und_live."Final" - od.strike_price
            ELSE NULL
        END AS moneyness_raw,

        CASE
            WHEN und_live."Final" IS NOT NULL
             AND und_live."Final" <> 0
             AND od.strike_price IS NOT NULL
            THEN (und_live."Final" - od.strike_price) / und_live."Final"
            ELSE NULL
        END AS moneyness_pct,

        CASE
            WHEN od.is_call = TRUE
             AND und_live."Final" IS NOT NULL
             AND od.strike_price IS NOT NULL
            THEN GREATEST(und_live."Final" - od.strike_price, 0)
            WHEN od.is_put = TRUE
             AND und_live."Final" IS NOT NULL
             AND od.strike_price IS NOT NULL
            THEN GREATEST(od.strike_price - und_live."Final", 0)
            ELSE NULL
        END AS intrinsic_value_live,

        CASE
            WHEN opt_live."Final" IS NOT NULL
             AND od.is_call = TRUE
             AND und_live."Final" IS NOT NULL
             AND od.strike_price IS NOT NULL
            THEN opt_live."Final" - GREATEST(und_live."Final" - od.strike_price, 0)

            WHEN opt_live."Final" IS NOT NULL
             AND od.is_put = TRUE
             AND und_live."Final" IS NOT NULL
             AND od.strike_price IS NOT NULL
            THEN opt_live."Final" - GREATEST(od.strike_price - und_live."Final", 0)

            ELSE NULL
        END AS extrinsic_value_live

    FROM public.option_detail od
    LEFT JOIN latest_live opt_live
        ON REPLACE(REPLACE(TRIM(opt_live."Ticker"), 'ي', 'ی'), 'ك', 'ک')
         = REPLACE(REPLACE(TRIM(od.stock_ticker), 'ي', 'ی'), 'ك', 'ک')
    LEFT JOIN latest_ob opt_ob
        ON REPLACE(REPLACE(TRIM(opt_ob."Symbol"), 'ي', 'ی'), 'ك', 'ک')
         = REPLACE(REPLACE(TRIM(od.stock_ticker), 'ي', 'ی'), 'ك', 'ک')
    LEFT JOIN latest_live und_live
        ON REPLACE(REPLACE(TRIM(und_live."Ticker"), 'ي', 'ی'), 'ك', 'ک')
         = REPLACE(REPLACE(TRIM(od.underlying_ticker), 'ي', 'ی'), 'ك', 'ک')
    LEFT JOIN latest_ob und_ob
        ON REPLACE(REPLACE(TRIM(und_ob."Symbol"), 'ي', 'ی'), 'ك', 'ک')
         = REPLACE(REPLACE(TRIM(od.underlying_ticker), 'ي', 'ی'), 'ك', 'ک')
    WHERE od.is_active = TRUE;
    """)


def downgrade():
    op.execute("DROP VIEW IF EXISTS public.vw_option_live_enriched;")