import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv

# بارگذاری متغیرهای محیطی از .env
load_dotenv()

# گرفتن آدرس دیتابیس از متغیر محیطی
DB_URL = os.getenv("DB_URL")

# ساخت engine غیرهمزمان
engine = create_async_engine(DB_URL, echo=True)


# تعریف Base برای مدل‌ها
Base = declarative_base()

# ساخت Session
async_session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# تابع اتصال برای Dependency Injection (مثلاً در FastAPI)
def get_engine():
    return engine
