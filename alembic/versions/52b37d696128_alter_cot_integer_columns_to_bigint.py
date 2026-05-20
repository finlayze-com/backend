"""alter cot integer columns to bigint

Revision ID: 52b37d696128
Revises: bbd9108a7093
Create Date: 2026-05-18 10:18:09.508594

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '52b37d696128'
down_revision: Union[str, Sequence[str], None] = 'bbd9108a7093'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    DO $$
    DECLARE
        r RECORD;
    BEGIN
        FOR r IN
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'cot_disaggregated_futures_only'
              AND data_type = 'integer'
              AND column_name <> 'id'
        LOOP
            EXECUTE format(
                'ALTER TABLE public.cot_disaggregated_futures_only
                 ALTER COLUMN %I TYPE BIGINT USING %I::BIGINT',
                r.column_name,
                r.column_name
            );
        END LOOP;
    END $$;
    """)


def downgrade():
    pass