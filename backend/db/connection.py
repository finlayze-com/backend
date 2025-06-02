import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# بارگذاری متغیرهای محیطی از .env
load_dotenv()

# گرفتن آدرس دیتابیس از متغیر محیطی
DB_URL = os.getenv("DB_URL")

# ساخت engine
engine = create_engine(DB_URL)

# تعریف Base برای مدل‌ها
Base = declarative_base()

# ساخت Session
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# تابع اتصال برای Dependency Injection (مثلاً در FastAPI)
def get_engine():
    return engine
