# main.py (در مسیر cron_jobs)

import subprocess

print("🚀 شروع اجرای آپدیت‌های روزانه و هفتگی")

# اجرای اسکریپت‌های روزانه
subprocess.run(["python", "cron_jobs/otherImportantFile/dollarfinal.py"])
subprocess.run(["python", "cron_jobs/daily/update_daily_data.py"])
subprocess.run(["python", "cron_jobs/daily/update_daily_indicator_for_All_Data.py"])
subprocess.run(["python", "cron_jobs/daily/update_daily_haghighi.py"])
subprocess.run(["python", "cron_jobs/daily/run_daily_join_sql.py"])
subprocess.run(["python", "cron_jobs/daily/SafKharid.py"])
# اجرای اسکریپت‌های هفتگی (فقط روز پنجشنبه مثلاً)
subprocess.run(["python", "cron_jobs/weekly/update_weekly_data.py"])
subprocess.run(["python", "cron_jobs/weekly/update_weekly_indicator_for_All_Data.py"])
subprocess.run(["python", "cron_jobs/weekly/update_weekly_haghighi.py"])
subprocess.run(["python", "cron_jobs/weekly/run_weekly_join_sql.py"])

print("✅ همه فایل‌ها اجرا شدند.")
