"""cleanup_null_orderbook_rows

Revision ID: 40b46e7f3851
Revises: 9880629f91e7
Create Date: 2025-11-20 14:27:59.350392

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '40b46e7f3851'
down_revision: Union[str, Sequence[str], None] = '9880629f91e7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute(
        """
        DELETE FROM orderbook_snapshot
        WHERE
            "BuyVolume1"  IS NULL AND
            "BuyVolume2"  IS NULL AND
            "BuyVolume3"  IS NULL AND
            "BuyVolume4"  IS NULL AND
            "BuyVolume5"  IS NULL AND
            "SellVolume1" IS NULL AND
            "SellVolume2" IS NULL AND
            "SellVolume3" IS NULL AND
            "SellVolume4" IS NULL AND
            "SellVolume5" IS NULL;
        """
    )


def downgrade():
    # این عملیات قابل برگردوندن نیست (چون ردیف‌های حذف‌شده برنمی‌گردن)
    pass