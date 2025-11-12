"""shareholder

Revision ID: 606d90ebfeef
Revises: 6def3fca09d9
Create Date: 2025-11-11 15:57:49.399875

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '606d90ebfeef'
down_revision: Union[str, Sequence[str], None] = '6def3fca09d9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.create_table(
        "shareholders_intervals",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),

        # شناسه نماد
        sa.Column("inscode", sa.String(20), nullable=False, index=True),
        sa.Column("symbol", sa.String(32), nullable=True),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("sector", sa.Text, nullable=True),

        # تاریخ‌ها
        sa.Column("start_d_even", sa.Integer, nullable=False, index=True),   # YYYYMMDD (میلادی)
        sa.Column("end_d_even", sa.Integer, nullable=True, index=True),      # NULL یعنی هنوز فعال
        sa.Column("start_date", sa.Date, nullable=False),
        sa.Column("end_date", sa.Date, nullable=True),

        # برای گزارش و ایندکس‌گذاری سریع:
        sa.Column("d_even", sa.Integer, nullable=False, index=True),         # = start_d_even (alias for convenience)
        sa.Column("date", sa.Date, nullable=False, index=True),              # = start_date  (alias for convenience)

        # وضعیت سهامدار
        sa.Column("holder_name", sa.String(512), nullable=False),
        sa.Column("holder_code", sa.String(64), nullable=True),
        sa.Column("isin", sa.String(20), nullable=True),

        sa.Column("shares", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("percent", sa.Numeric(8, 4), nullable=False, server_default="0"),

        # اطلاعات کمکی
        sa.Column("change_flag", sa.SmallInteger, nullable=True),
        sa.Column("change_amount", sa.BigInteger, nullable=True),

        # بازار و مارکت‌کپ
        sa.Column("marketcap", sa.Numeric(20, 2), nullable=True),
        sa.Column("marketcap_usd", sa.Numeric(20, 2), nullable=True),

        sa.Column("source", sa.String(32), nullable=False, server_default="tsetmc_api"),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),

        # هر سهامدار در یک نماد فقط یک بازه با start_d_even یکتا دارد
        sa.UniqueConstraint("inscode", "holder_name", "start_d_even", name="uq_si_ins_holder_start"),
    )

    op.create_index("ix_si_ins_holder_active", "shareholders_intervals", ["inscode", "holder_name", "end_d_even"])
    op.create_index("ix_si_ins_active_ondate", "shareholders_intervals", ["inscode", "start_d_even", "end_d_even"])


def downgrade():
    op.drop_index("ix_si_ins_active_ondate", table_name="shareholders_intervals")
    op.drop_index("ix_si_ins_holder_active", table_name="shareholders_intervals")
    op.drop_table("shareholders_intervals")