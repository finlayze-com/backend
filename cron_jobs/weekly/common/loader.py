# -*- coding: utf-8 -*-
"""
Loader utilities for weekly data builder.
Loads data (daily tables, dollar data, etc.) from PostgreSQL.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy import text
import pandas as pd

def get_engine():
    """
    ایجاد اتصال SQLAlchemy از DB_URL یا DB_URL_SYNC در .env
    """
    env_paths = [
        os.path.join(os.path.dirname(__file__), "../../../.env"),
        os.path.join(os.path.dirname(__file__), "../../../../.env"),
    ]
    for p in env_paths:
        if os.path.exists(p):
            load_dotenv(p)
            break
    db_url = os.getenv("DB_URL_SYNC") or os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("❌ DB_URL or DB_URL_SYNC not found in .env")
    return create_engine(db_url)

def load_table(engine, table_name: str):
    """
    بارگذاری کامل یک جدول از دیتابیس.
    """
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)

def load_dollar_data(engine):
    """
    خواندن نرخ دلار برای نگاشت دلاری.
    فرض بر این است که ستون‌ها شامل date_miladi و close هستند.
    """
    df = pd.read_sql("SELECT date_miladi, close FROM dollar_data ORDER BY date_miladi", engine)
    df.rename(columns={"close": "dollar_rate"}, inplace=True)
    df["date_miladi"] = pd.to_datetime(df["date_miladi"])
    return df.set_index("date_miladi").sort_index()


def get_last_week_end(engine, weekly_table: str):
    """
    آخرین week_end ذخیره‌شده در جدول هفتگی.
    """
    query = text(f"SELECT MAX(week_end) FROM {weekly_table}")
    with engine.connect() as conn:
        result = conn.execute(query)
        value = result.scalar_one_or_none()
    if not value:
        return pd.Timestamp("1900-01-01")
    return pd.to_datetime(value)
