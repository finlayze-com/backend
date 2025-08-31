# cron_jobs/daily/common/writers.py
from typing import Iterable
from psycopg2.extras import execute_batch

COLUMNS_SQL = """
    stock_ticker, j_date, date_miladi, weekday,
    open, high, low, close, final_price,
    volume, value, trade_count, name, market,
    adjust_open, adjust_high, adjust_low, adjust_close, adjust_final_price,
    dollar_rate, adjust_open_usd, adjust_high_usd, adjust_low_usd, adjust_close_usd, value_usd
"""

def insert_daily_rows(cur, table_name: str, records: Iterable[tuple]):
    insert_sql = f"""
        INSERT INTO {table_name} (
            {COLUMNS_SQL}
        ) VALUES (
            %s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (stock_ticker, date_miladi) DO NOTHING;
    """
    execute_batch(cur, insert_sql, list(records), page_size=500)
