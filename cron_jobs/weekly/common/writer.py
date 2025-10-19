# -*- coding: utf-8 -*-
"""
Writer utilities for weekly updater and indicators.
Handles UPSERT operations safely and efficiently.
"""

from sqlalchemy import text

def upsert_dataframe(df, engine, table_name: str, conflict_cols=("stock_ticker", "week_end")):
    """
    درج یا آپدیت داده‌ها با ON CONFLICT DO UPDATE
    """
    if df.empty:
        print(f"⚠️ No new rows to insert into {table_name}")
        return

    cols = list(df.columns)
    placeholders = ", ".join([f":{c}" for c in cols])
    col_list = ", ".join(cols)
    conflict_str = ", ".join(conflict_cols)

    update_str = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in conflict_cols])

    sql = f"""
    INSERT INTO {table_name} ({col_list})
    VALUES ({placeholders})
    ON CONFLICT ({conflict_str}) DO UPDATE SET
    {update_str}
    """
    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict(orient="records"))

    print(f"✅ {len(df)} rows upserted into {table_name}.")
