"""quote: add unique key, cast Time to time, add indexes (no raw cols)

Revision ID: 5cc200fd007f
Revises: 210b850393a8
Create Date: 2025-11-10 07:01:47.534337

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5cc200fd007f'
down_revision: Union[str, Sequence[str], None] = '210b850393a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


TABLE = "quote"

def upgrade():
    # Time -> time
    op.execute('ALTER TABLE "quote" ALTER COLUMN "Time" TYPE time USING "Time"::time')

    # کلید یکتا برای upsert
    op.create_unique_constraint("quote_inscode_date_unique", TABLE, ["inscode", "date"])

    # ایندکس‌ها
    op.create_index("idx_quote_date", TABLE, ["date"])
    op.create_index("idx_quote_bq_desc", TABLE, ["BQ_Value"], postgresql_using="btree")
    op.create_index("idx_quote_sq_desc", TABLE, ["SQ_Value"], postgresql_using="btree")

def downgrade():
    op.drop_index("idx_quote_sq_desc", table_name=TABLE)
    op.drop_index("idx_quote_bq_desc", table_name=TABLE)
    op.drop_index("idx_quote_date", table_name=TABLE)
    op.drop_constraint("quote_inscode_date_unique", TABLE, type_="unique")
    op.execute('ALTER TABLE "quote" ALTER COLUMN "Time" TYPE text USING "Time"::text')