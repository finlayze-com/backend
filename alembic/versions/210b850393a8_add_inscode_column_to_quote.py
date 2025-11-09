"""add inscode column to quote

Revision ID: 210b850393a8
Revises: 847ba7f6043f
Create Date: 2025-11-09 18:21:26.754901

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '210b850393a8'
down_revision: Union[str, Sequence[str], None] = '847ba7f6043f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # ➊ افزودن ستون جدید
    op.add_column('quote', sa.Column('inscode', sa.Text(), nullable=True))
    op.add_column("quote", sa.Column("downloaded_at", sa.DateTime(timezone=True), nullable=True))

    # ➋ ساخت ایندکس برای سرعت در جویـن‌ها و فیلترها
    op.create_index('idx_quote_inscode_date', 'quote', ['inscode', 'date'], unique=False)
    op.create_index("idx_quote_downloaded_at", "quote", ["downloaded_at"], unique=False)

    # ➌ (اختیاری) اگر کلید یکتا می‌خوای براساس inscode/date:
    # op.create_unique_constraint('uq_quote_inscode_date', 'quote', ['inscode', 'date'])


def downgrade():
    # حذف ایندکس و ستون در صورت rollback
    op.drop_index('idx_quote_inscode_date', table_name='quote')
    op.drop_index("idx_quote_downloaded_at", table_name="quote")
    # op.drop_constraint('uq_quote_inscode_date', 'quote', type_='unique')  # اگر ایجادش کرده بودی
    op.drop_column('quote', 'inscode')
    op.drop_column("quote", "downloaded_at")
