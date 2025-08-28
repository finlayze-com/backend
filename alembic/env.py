# alembic/env.py
from __future__ import annotations
import os, sys
from logging.config import fileConfig
from alembic import context
from sqlalchemy import engine_from_config, pool

# --- logging ---
config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)

# --- ensure project is importable ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# --- load .env (DB_URL_SYNC / DATABASE_URL / DB_URL_ASYNC) ---
try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv()  # خواندن .env از ریشه پروژه
except Exception:
    # اگر python-dotenv نصب نبود، مشکلی نیست؛ از متغیرهای محیطی سیستم استفاده می‌شود
    pass

# اولویت: DB_URL_SYNC > DATABASE_URL > DB_URL_ASYNC
db_url_sync = os.getenv("DB_URL_SYNC", "").strip()
db_url_any  = os.getenv("DATABASE_URL", "").strip()  # ممکن است async یا sync باشد
db_url_async = os.getenv("DB_URL_ASYNC", "").strip()

def to_sync_url(url: str) -> str:
    if not url:
        return url
    # اگر async بود، به psycopg2 تبدیل کن
    return (url
            .replace("postgresql+asyncpg://", "postgresql+psycopg2://")
            .replace("postgres+asyncpg://", "postgresql+psycopg2://"))

# انتخاب نهایی URL برای we (sync)
final_url = ""
if db_url_sync:
    final_url = db_url_sync  # مستقیماً از .env شما
elif db_url_any:
    final_url = to_sync_url(db_url_any)
elif db_url_async:
    final_url = to_sync_url(db_url_async)

if not final_url:
    # آخرین تلاش: از alembic.ini بخوان (اگر آنجا چیزی گذاشته باشی)
    final_url = config.get_main_option("sqlalchemy.url", "").strip()

if not final_url:
    raise RuntimeError(
        "No database URL found. Set DB_URL_SYNC (preferred) or DATABASE_URL/DB_URL_ASYNC "
        "in .env or environment variables."
    )

# ست‌کردن در alembic config
config.set_main_option("sqlalchemy.url", final_url)

# --- import Base & models so metadata is populated ---
from backend.db.connection import Base
import backend.users.models   as user_models
import backend.stocks.models  as stock_models
# اگر ماژول دیگری هم داری (funds و …) همین‌جا import کن

target_metadata = Base.metadata

# --- optional: filter objects for autogenerate ---
def include_object(obj, name, type_, reflected, compare_to):
    # مثال برای اسکپ کردن ویو/جداول ETL:
    # if type_ == "table" and name in {"daily_joined_data", "weekly_joined_data"}:
    #     return False
    return True

def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
