"""create cot asset mapping table

Revision ID: 306f514fc78b
Revises: b55f3bc45c26
Create Date: 2026-05-20 17:52:43.144888

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '306f514fc78b'
down_revision: Union[str, Sequence[str], None] = 'b55f3bc45c26'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade():

    op.create_table(
        "cot_asset_mapping",

        sa.Column(
            "id",
            sa.BigInteger(),
            primary_key=True,
            autoincrement=True
        ),

        sa.Column(
            "cftc_contract_market_code",
            sa.Text(),
            nullable=False
        ),

        sa.Column(
            "asset_group",
            sa.Text(),
            nullable=False
        ),

        sa.Column(
            "asset_code",
            sa.Text(),
            nullable=True
        ),

        sa.Column(
            "asset_name",
            sa.Text(),
            nullable=True
        ),

        sa.Column(
            "dashboard_order",
            sa.Integer(),
            nullable=True
        ),

        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default="true"
        ),

        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            server_default=sa.text("NOW()")
        ),
    )

    op.create_unique_constraint(
        "uq_cot_asset_mapping_contract",
        "cot_asset_mapping",
        ["cftc_contract_market_code"]
    )

    op.create_index(
        "idx_cot_asset_mapping_group",
        "cot_asset_mapping",
        ["asset_group"]
    )


def downgrade():

    op.drop_index(
        "idx_cot_asset_mapping_group",
        table_name="cot_asset_mapping"
    )

    op.drop_constraint(
        "uq_cot_asset_mapping_contract",
        "cot_asset_mapping",
        type_="unique"
    )

    op.drop_table(
        "cot_asset_mapping"
    )