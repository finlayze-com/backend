"""extend weekly_joined_data with haghighi USD columns

Revision ID: 6d967df5223b
Revises: 96fdfeacaf2c
Create Date: 2025-10-18 10:35:12.409010

"""
from typing import Sequence, Union
from sqlalchemy import text
from alembic import op
import sqlalchemy as sa
import re

# revision identifiers, used by Alembic.
revision: str = '6d967df5223b'
down_revision: Union[str, Sequence[str], None] = '96fdfeacaf2c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "public"
VIEW_NAME = "weekly_joined_data"
QUALIFIED_VIEW = f"{SCHEMA}.{VIEW_NAME}" if SCHEMA else VIEW_NAME

def _get_view_def(conn, qualified_view: str) -> str:
    # تعریف فعلی ویو را می‌خوانیم
    res = conn.execute(
        text("SELECT pg_get_viewdef(to_regclass(:v), true)"),
        {"v": qualified_view}
    ).scalar()
    if not res:
        raise RuntimeError(f"Could not fetch viewdef for {qualified_view}")
    sql = res.strip()
    # هر سِمی‌کالن انتهایی را حذف کن (مشکل CTE)
    sql = re.sub(r";\s*$", "", sql)
    return sql

def _create_or_replace_view(conn, sql_body: str) -> None:
    conn.execute(text(f"CREATE OR REPLACE VIEW {QUALIFIED_VIEW} AS {sql_body}"))

def upgrade() -> None:
    conn = op.get_bind()

    # 1) تعریف فعلی ویو
    base_select = _get_view_def(conn, QUALIFIED_VIEW)

    # 2) نسخه جدید: CTE + جوین صحیح به weekly_haghighi (alias: wh2)
    joined_select = f"""
WITH base AS (
{base_select}
)
SELECT
    base.*,
    wh2.buy_i_value_usd,
    wh2.buy_n_value_usd,
    wh2.sell_i_value_usd,
    wh2.sell_n_value_usd
FROM base
LEFT JOIN weekly_haghighi AS wh2
  ON  wh2.symbol   = base.stock_ticker
  AND wh2.week_end = base.week_end
"""
    _create_or_replace_view(conn, joined_select)

def downgrade() -> None:
    conn = op.get_bind()

    # تعریف فعلی (پس از upgrade) را بخوان
    current_def = _get_view_def(conn, QUALIFIED_VIEW)

    # محتوای CTE base را استخراج کنیم تا SELECT اصلیِ قبلی برگردد
    m = re.search(
        r"WITH\s+base\s+AS\s*\(\s*(?P<base>.*?)\s*\)\s*SELECT",
        current_def,
        re.IGNORECASE | re.DOTALL
    )
    if not m:
        raise RuntimeError("Could not locate CTE 'base' in the current view definition; downgrade aborted.")

    base_sql = m.group("base").strip()
    _create_or_replace_view(conn, base_sql)
