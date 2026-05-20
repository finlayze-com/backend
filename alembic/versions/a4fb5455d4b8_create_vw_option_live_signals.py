"""create vw_option_live_signals

Revision ID: a4fb5455d4b8
Revises: 1a330715e9e4
Create Date: 2026-05-03 12:05:49.758293

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a4fb5455d4b8'
down_revision: Union[str, Sequence[str], None] = '1a330715e9e4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade():
    op.execute("""
    CREATE OR REPLACE VIEW public.vw_option_live_signals AS
    WITH latest_hv AS (
        SELECT DISTINCT ON (ticker)
            ticker,
            trade_date AS hv_trade_date,
            hv_10d,
            hv_20d,
            hv_30d,
            hv_60d
        FROM public.underlying_volatility_daily
        ORDER BY ticker, trade_date DESC
    )
    SELECT
        vle.*,

        hv.hv_trade_date,
        hv.hv_10d,
        hv.hv_20d,
        hv.hv_30d,
        hv.hv_60d,

        CASE
            WHEN hv.hv_20d BETWEEN 0.05 AND 2 THEN hv.hv_20d
            ELSE NULL
        END AS hv_20d_clean,

        CASE
            WHEN vle.option_bid_price IS NOT NULL
             AND vle.option_ask_price IS NOT NULL
             AND vle.option_bid_price > 0
             AND vle.option_ask_price > 0
            THEN (vle.option_bid_price + vle.option_ask_price) / 2.0
            ELSE vle.option_last_price
        END AS option_mid_price,

        CASE
            WHEN vle.option_volume IS NOT NULL AND vle.option_volume > 0 THEN TRUE
            ELSE FALSE
        END AS has_live_trade,

        CASE
            WHEN vle.option_bid_price IS NOT NULL
             AND vle.option_ask_price IS NOT NULL
             AND vle.option_bid_price > 0
             AND vle.option_ask_price > 0
            THEN TRUE
            ELSE FALSE
        END AS has_live_orderbook,

        CASE
            WHEN vle.option_spread_pct IS NOT NULL
             AND vle.option_spread_pct <= 0.15
            THEN TRUE
            ELSE FALSE
        END AS has_acceptable_spread,

        CASE
            WHEN vle.option_last_price IS NOT NULL
             AND vle.spot_price IS NOT NULL
             AND vle.strike_price IS NOT NULL
             AND vle.days_to_expiry > 0
             AND hv.hv_20d BETWEEN 0.05 AND 2
            THEN TRUE
            ELSE FALSE
        END AS is_live_signal_ready,

        CASE
            WHEN vle.option_last_price IS NULL THEN 'NO_OPTION_PRICE'
            WHEN vle.spot_price IS NULL THEN 'NO_SPOT_PRICE'
            WHEN vle.days_to_expiry IS NULL OR vle.days_to_expiry <= 0 THEN 'EXPIRED_OR_INVALID_EXPIRY'
            WHEN hv.hv_20d IS NULL THEN 'NO_HV'
            WHEN hv.hv_20d <= 0.05 OR hv.hv_20d > 2 THEN 'BAD_HV'
            WHEN vle.option_spread_pct IS NOT NULL AND vle.option_spread_pct > 0.15 THEN 'WIDE_SPREAD'
            ELSE 'OK'
        END AS live_data_status,

        CASE
            WHEN vle.moneyness_pct IS NULL THEN NULL
            WHEN vle.is_call = TRUE AND vle.moneyness_pct > 0.03 THEN 'ITM'
            WHEN vle.is_call = TRUE AND vle.moneyness_pct BETWEEN -0.03 AND 0.03 THEN 'ATM'
            WHEN vle.is_call = TRUE AND vle.moneyness_pct < -0.03 THEN 'OTM'

            WHEN vle.is_put = TRUE AND vle.moneyness_pct < -0.03 THEN 'ITM'
            WHEN vle.is_put = TRUE AND vle.moneyness_pct BETWEEN -0.03 AND 0.03 THEN 'ATM'
            WHEN vle.is_put = TRUE AND vle.moneyness_pct > 0.03 THEN 'OTM'
            ELSE NULL
        END AS moneyness_bucket,

        CASE
            WHEN vle.extrinsic_value_live IS NULL THEN NULL
            WHEN vle.extrinsic_value_live < 0 THEN 'BAD_PRICE'
            WHEN vle.extrinsic_value_live = 0 THEN 'NO_TIME_VALUE'
            ELSE 'HAS_TIME_VALUE'
        END AS extrinsic_status,

        CASE
            WHEN vle.option_spread_pct IS NOT NULL
             AND vle.option_spread_pct <= 0.05 THEN 'TIGHT'
            WHEN vle.option_spread_pct IS NOT NULL
             AND vle.option_spread_pct <= 0.15 THEN 'NORMAL'
            WHEN vle.option_spread_pct IS NOT NULL
             AND vle.option_spread_pct > 0.15 THEN 'WIDE'
            ELSE NULL
        END AS spread_bucket,

        CASE
            WHEN vle.option_last_price IS NOT NULL
             AND vle.spot_price IS NOT NULL
             AND vle.strike_price IS NOT NULL
             AND vle.days_to_expiry > 0
             AND hv.hv_20d BETWEEN 0.05 AND 2
             AND (vle.option_spread_pct IS NULL OR vle.option_spread_pct <= 0.15)
             AND vle.extrinsic_value_live IS NOT NULL
             AND vle.extrinsic_value_live >= 0
            THEN TRUE
            ELSE FALSE
        END AS is_trade_candidate

    FROM public.vw_option_live_enriched vle
    LEFT JOIN latest_hv hv
        ON REPLACE(REPLACE(TRIM(hv.ticker), 'ي', 'ی'), 'ك', 'ک')
         = REPLACE(REPLACE(TRIM(vle.underlying_ticker), 'ي', 'ی'), 'ك', 'ک');
    """)


def downgrade():
    op.execute("DROP VIEW IF EXISTS public.vw_option_live_signals;")