from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.stocks.models import Base
from dotenv import load_dotenv
import os
import logging

# بارگذاری متغیرهای محیطی از .env
load_dotenv()

# خواندن مقادیر از محیط
DB_NAME = os.getenv("POSTGRES_DB")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# ساخت آدرس اتصال
DATABASE_URL = f"postgresql://{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ساخت Engine
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        print("✅ جداول با موفقیت ساخته شدند.")
    except Exception as e:
        logging.exception("❌ خطا در ساخت جداول:")
        raise e

if __name__ == "__main__":
    init_db()
