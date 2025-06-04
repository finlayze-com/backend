from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.stocks.models import Base
from dotenv import load_dotenv
import os
import logging

load_dotenv()

DATABASE_URL = os.getenv("DB_URL")  # ✅ مستقیماً از .env بخون

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
