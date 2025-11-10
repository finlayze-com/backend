"""base_value

Revision ID: 6def3fca09d9
Revises: 5cc200fd007f
Create Date: 2025-11-10 09:06:51.003778

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6def3fca09d9'
down_revision: Union[str, Sequence[str], None] = '5cc200fd007f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # ➊ افزودن ستون
    op.add_column(
        "quote",
        sa.Column("base_value", sa.Numeric(30, 0), nullable=True)
    )
    # ➋ ایندکس (اختیاری اما مفید اگر زیاد رویش فیلتر/سورت می‌کنی)
    op.create_index(
        "idx_quote_date_base_value",
        "quote",
        ["date", "base_value"],
        unique=False
    )

def downgrade():
    op.drop_index("idx_quote_date_base_value", table_name="quote")
    op.drop_column("quote", "base_value")