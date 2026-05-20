"""extend cot asset mapping table

Revision ID: 2c90840cbdd5
Revises: 306f514fc78b
Create Date: 2026-05-20 18:02:43.819514

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2c90840cbdd5'
down_revision: Union[str, Sequence[str], None] = '306f514fc78b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade():
    op.add_column(
        "cot_asset_mapping",
        sa.Column("market_and_exchange_names", sa.Text(), nullable=True)
    )

    op.add_column(
        "cot_asset_mapping",
        sa.Column("asset_subgroup", sa.Text(), nullable=True)
    )


def downgrade():
    op.drop_column("cot_asset_mapping", "asset_subgroup")
    op.drop_column("cot_asset_mapping", "market_and_exchange_names")