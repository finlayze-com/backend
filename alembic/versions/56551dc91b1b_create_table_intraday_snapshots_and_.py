"""create_table-intraday_snapshots and market_snapshot

Revision ID: 56551dc91b1b
Revises: c9a69ebbf8cc
Create Date: 2026-02-10 10:00:02.945140

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '56551dc91b1b'
down_revision: Union[str, Sequence[str], None] = 'c9a69ebbf8cc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


"""create intraday market and sector snapshot tables

Revision ID: 20260210_intraday_snapshots
Revises: <PUT_PREVIOUS_REVISION_HERE>
Create Date: 2026-02-10
"""


def upgrade():
    # ------------------------------------------------------------------
    # market_intraday_snapshot
    # ------------------------------------------------------------------
    op.create_table(
        "market_intraday_snapshot",
        sa.Column("ts", sa.DateTime(timezone=True), primary_key=True),
        sa.Column("snapshot_day", sa.Date(), nullable=False),

        sa.Column("symbols_count", sa.Integer()),
        sa.Column("green_ratio", sa.Numeric(5, 4)),
        sa.Column("eqw_avg_ret_pct", sa.Numeric(7, 3)),

        sa.Column("total_value", sa.BigInteger()),
        sa.Column("total_volume", sa.BigInteger()),
        sa.Column("net_real_value", sa.BigInteger()),
        sa.Column("net_legal_value", sa.BigInteger()),

        sa.Column("imbalance5", sa.Numeric(7, 4)),
        sa.Column("imbalance_state", sa.Text()),

        sa.Column("source", sa.Text(), server_default="live_pipeline"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_index(
        "idx_market_intraday_day_ts",
        "market_intraday_snapshot",
        ["snapshot_day", "ts"],
        unique=False,
    )

    # ------------------------------------------------------------------
    # sector_intraday_snapshot
    # ------------------------------------------------------------------
    op.create_table(
        "sector_intraday_snapshot",
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("snapshot_day", sa.Date(), nullable=False),

        sa.Column("sector_key", sa.Text(), nullable=False),
        sa.Column("sector_name", sa.Text()),

        sa.Column("symbols_count", sa.Integer()),
        sa.Column("green_ratio", sa.Numeric(5, 4)),

        sa.Column("total_value", sa.BigInteger()),
        sa.Column("total_volume", sa.BigInteger()),
        sa.Column("net_real_value", sa.BigInteger()),
        sa.Column("net_legal_value", sa.BigInteger()),

        sa.Column("imbalance5", sa.Numeric(7, 4)),
        sa.Column("imbalance_state", sa.Text()),

        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),

        sa.PrimaryKeyConstraint("ts", "sector_key"),
    )

    op.create_index(
        "idx_sector_intraday_day_key_ts",
        "sector_intraday_snapshot",
        ["snapshot_day", "sector_key", "ts"],
        unique=False,
    )

    op.create_index(
        "idx_sector_intraday_day_ts",
        "sector_intraday_snapshot",
        ["snapshot_day", "ts"],
        unique=False,
    )


def downgrade():
    op.drop_index("idx_sector_intraday_day_ts", table_name="sector_intraday_snapshot")
    op.drop_index("idx_sector_intraday_day_key_ts", table_name="sector_intraday_snapshot")
    op.drop_table("sector_intraday_snapshot")

    op.drop_index("idx_market_intraday_day_ts", table_name="market_intraday_snapshot")
    op.drop_table("market_intraday_snapshot")
