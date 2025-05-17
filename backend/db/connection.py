# backend/db/connection.py

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# آدرس دیتابیس
DB_URI = "postgresql://postgres:Afiroozi12@localhost:5432/postgres1"

# ساخت engine
engine = create_engine(DB_URI)

# تعریف Base برای مدل‌ها
Base = declarative_base()

# ساخت Session
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# تابع اتصال برای Dependency Injection
def get_engine():
    return engine
