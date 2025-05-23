import os
import sys
import time
from datetime import datetime

# اضافه کردن پوشه Dashboard به sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

# ایمپورت توابع از مسیر واقعی
from cron_jobs.daily.update_dollar_data import update_today_dollar
from cron_jobs.daily.update_daily_data import update_daily_data
from cron_jobs.daily.update_daily_indicator_for_All_Data import build_daily_indicators
from cron_jobs.daily.update_daily_haghighi import update_haghighi_data
from cron_jobs.weekly.update_weekly_data import update_weekly_data
from cron_jobs.weekly.update_weekly_indicator_for_All_Data import build_weekly_indicators
from cron_jobs.weekly.update_weekly_haghighi import weekly_haghighi_data
from database.config import DB_CONFIG


def log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{now}] {msg}"
    print(full_msg)
    with open("log_main.txt", "a", encoding="utf-8") as f:
        f.write(full_msg + "\n")


def run_stage(name, func):
    log(f"🚀 شروع مرحله: {name}")
    start = time.time()
    try:
        func()
        duration = time.time() - start
        log(f"✅ پایان مرحله: {name} (زمان: {duration:.2f} ثانیه)")
    except Exception as e:
        log(f"❌ خطا در مرحله {name}: {e}")


def run_sql_file(path):
    import psycopg2
    # مسیر کامل فایل SQL
    full_path = os.path.join(project_root, path)

    with open(full_path, "r", encoding="utf-8") as f:
        sql = f.read()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    log(f"📄 SQL اجرا شد: {os.path.basename(path)}")


# مراحل به ترتیب اجرای Pipeline
def main():
    log("🚦 شروع کامل Pipeline آپدیت داده‌ها")

    run_stage("1. آپدیت نرخ دلار", update_today_dollar)
    run_stage("2. آپدیت دیتای روزانه", update_daily_data)
    run_stage("4. آپدیت داده‌های حقیقی روزانه", update_haghighi_data)
    run_stage("3. ساخت اندیکاتورهای روزانه", build_daily_indicators)
    run_stage("5. ساخت جدول نهایی روزانه", lambda: run_sql_file("cron_jobs/daily/join_daily.sql"))
    run_stage("6. آپدیت دیتای هفتگی", update_weekly_data)
    run_stage("7. ساخت اندیکاتورهای هفتگی", build_weekly_indicators)
    run_stage("8. آپدیت حقیقی‌های هفتگی", weekly_haghighi_data)
    run_stage("9. ساخت جدول نهایی هفتگی", lambda: run_sql_file("cron_jobs/weekly/join_weekly.sql"))

    log("🎉 پایان کل فرآیند!")


if __name__ == "__main__":
    main()
