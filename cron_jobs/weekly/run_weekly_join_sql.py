import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def run_weekly_join():
    # بارگذاری متغیرهای محیطی
    dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
    load_dotenv(dotenv_path)
    db_url = os.getenv("DB_URL")

    # اتصال به دیتابیس
    engine = create_engine(db_url)

    # مسیر فایل SQL
    sql_file_path = os.path.join(os.path.dirname(__file__), 'join_weekly.sql')

    print("🚀 اجرای فایل SQL برای ساخت جدول weekly_joined_data...")

    try:
        with engine.connect() as conn:
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql_commands = file.read()
                conn.execute(text(sql_commands))
        print("✅ اجرای فایل SQL با موفقیت انجام شد.")
    except Exception as e:
        print(f"❌ خطا در اجرای فایل SQL: {e}")

if __name__ == "__main__":
    run_weekly_join()
