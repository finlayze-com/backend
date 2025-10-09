# cron_jobs/main.py
import os
import sys
import subprocess
import logging
from pathlib import Path
from typing import List, Tuple, Dict

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# =========================
# تنظیمات عمومی
# =========================
# تایم‌زون اجرا (می‌تونی در .env مقدار APP_TZ رو ست کنی)
APP_TZ = os.getenv("APP_TZ", "Asia/Tehran")

# پنجره‌ی اجرای خودکار: همه‌ی تسک‌هایی که زمان سفارشی ندارند
# از 20:00 با فاصله‌ی 1 دقیقه شروع می‌شوند.
WINDOW_START_HOUR = 17
WINDOW_START_MINUTE = 10
SPACING_MINUTES = 2  # فاصله بین تسک‌ها دقیقاً یک دقیقه

# حداکثر دفعات تلاش مجدد برای هر تسک
RETRY_TIMES = 2

# مسیرهای پایه
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# لاگینگ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "main.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("scheduler")


# =========================
# کمک‌تابع اجرای اسکریپت‌ها
# =========================
def run_script(py_path: Path, args: List[str] = None, name: str = "") -> None:
    """
    اجرای اسکریپت پایتون با همین مفسر فعلی (sys.executable) + ری‌تری در صورت خطا.
    خروجی STDOUT/STDERR لاگ می‌شود.
    """
    args = args or []
    full_cmd = [sys.executable, str(py_path), *args]
    attempt = 0
    while True:
        attempt += 1
        try:
            logger.info(f"▶️  START [{name}] → {py_path} (attempt {attempt})")
            completed = subprocess.run(full_cmd, check=True, capture_output=True, text=True)
            if completed.stdout:
                logger.info(f"[{name}] STDOUT:\n{completed.stdout}")
            if completed.stderr:
                logger.warning(f"[{name}] STDERR:\n{completed.stderr}")
            logger.info(f"✅ DONE [{name}]")
            break
        except subprocess.CalledProcessError as e:
            logger.error(
                f"❌ FAIL [{name}] attempt {attempt}: {e}\n"
                f"STDOUT:\n{e.stdout}\n"
                f"STDERR:\n{e.stderr}"
            )
            if attempt > RETRY_TIMES:
                logger.error(f"⛔ GIVING UP [{name}] after {RETRY_TIMES} retries.")
                break


# =========================
# تعریف کامل تسک‌ها (مسیرها)
# اگر فایلِ اشاره‌شده وجود نداشته باشد، فقط هشدار لاگ می‌شود و ادامه می‌دهد.
# =========================

# کارهای دیتای روزانه/گروه‌ها (به ترتیب دلخواه)
DAILY_TASKS: List[Tuple[str, Path]] = [
    # --- مقدمات روزانه
    ("dollar",                PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "update_dollar.py"),      # قبلاً dollar.py
    ("update_daily_haghighi", PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "update_haghighi.py"),

    # --- گروه‌های اصلی بازار
    ("run_saham",             PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_saham.py"),
    ("run_retail",            PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_retail.py"),
    ("run_block",             PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_block.py"),
    ("run_fund",              PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_fund.py"),

    # --- زیرگروه‌های صندوق (در صورت نیاز)
    ("run_fund_stock",        PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_fund_stock.py"),
    ("run_fund_balanced",     PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_fund_balanced.py"),
    ("run_fund_fixincome",    PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_fund_fixincome.py"),
    ("run_fund_gold",         PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_fund_gold.py"),
    ("run_fund_index_stock",  PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_fund_index_stock.py"),
    ("run_fund_leverage",     PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_fund_leverage.py"),
    ("run_fund_other",        PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_fund_other.py"),
    ("run_fund_segment",      PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_fund_segment.py"),
    ("run_fund_zafran",       PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_fund_zafran.py"),

    # --- ابزارهای دیگر
    ("run_option",            PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_option.py"),
    ("run_kala",              PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_kala.py"),
    ("run_tamin",             PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "groups" / "run_tamin.py"),
]

# کارهای اندیکاتور (ساعت 20 به بعد با فاصله 1 دقیقه؛ مگر این‌که CUSTOM داشته باشند)
INDICATOR_TASKS: List[Tuple[str, Path]] = [
    ("run_fund_gold_Ind",         PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "indicators" / "run_fund_gold_Ind.py"),
    ("run_fund_index_stock_ind",  PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "indicators" / "run_fund_index_stock_ind.py"),
    ("run_fund_leverage_ind",     PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "indicators" / "run_fund_leverage_ind.py"),
    ("run_saham_ind",             PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "indicators" / "run_saham_ind.py"),
    ("run_fund_segment_ind",      PROJECT_ROOT / "cron_jobs" / "daily" / "common" / "indicators" / "run_fund_segment_ind.py"),
]


# =========================
# زمان‌بندی سفارشی هر تسک (اختیاری)
# اگر برای یک تسک این‌جا hour/minute تعیین شود،
# آن تسک دیگر وارد صف 20–21 نمی‌شود و دقیقاً در همان ساعت سفارشی اجرا خواهد شد.
# ⚠️ طبق درخواست: run_option به صورت سفارشی زمان‌بندی شد (پیش‌فرض 19:55).
# =========================
CUSTOM_SCHEDULE: Dict[str, Dict[str, int]] = {
    "run_option": {"hour": 19, "minute": 55},  # ← هر زمان خواستی عوضش کن (مثلاً 20:45)
    # مثال‌ها:
    # "run_saham": {"hour": 19, "minute": 45},
    # "run_fund_leverage_ind": {"hour": 21, "minute": 5},
}


# =========================
# توابع زمان‌بندی
# =========================
def _make_task(name: str, path: Path):
    """
    یک کلوزر برمی‌گرداند تا در زمان‌بندی فراخوانی شود.
    وجود فایل را چک می‌کند؛ سپس اجرا می‌کند.
    """
    def _task():
        logger.info(f"🗓️  Scheduled run for [{name}]")
        if not path.exists():
            logger.error(f"⚠️  Script not found for [{name}]: {path}")
            return
        run_script(path, name=name)
    return _task


def schedule_in_window_with_offsets(
    sched: BlockingScheduler,
    tasks: List[Tuple[str, Path]],
    start_hour: int,
    start_minute: int,
    spacing_minutes: int,
    window_label: str,
):
    """
    تسک‌هایی که در CUSTOM_SCHEDULE نیستند را از start_hour:start_minute
    با فاصله‌ی spacing_minutes دقیقه زمان‌بندی می‌کند.
    """
    hour = start_hour
    minute = start_minute
    idx = 0
    for name, path in tasks:
        # اگر زمان سفارشی دارد، جدا زمان‌بندی می‌شود
        if name in CUSTOM_SCHEDULE:
            custom = CUSTOM_SCHEDULE[name]
            h, m = custom["hour"], custom["minute"]
            sched.add_job(
                _make_task(name, path),
                CronTrigger(hour=h, minute=m, timezone=APP_TZ),
                id=f"{window_label}_custom_{name}",
                replace_existing=True,
                misfire_grace_time=60 * 30,
                max_instances=1,
            )
            logger.info(f"⏰ [{name}] scheduled by CUSTOM at {h:02d}:{m:02d} {APP_TZ}")
            continue

        # محاسبه‌ی ساعت/دقیقه با فاصله‌های 1 دقیقه‌ای
        add_minutes = idx * spacing_minutes
        h = (hour + (minute + add_minutes) // 60) % 24
        m = (minute + add_minutes) % 60

        sched.add_job(
            _make_task(name, path),
            CronTrigger(hour=h, minute=m, timezone=APP_TZ),
            id=f"{window_label}_{idx}_{name}",
            replace_existing=True,
            misfire_grace_time=60 * 30,
            max_instances=1,
        )
        logger.info(f"⏰ [{name}] scheduled at {h:02d}:{m:02d} {APP_TZ}")
        idx += 1


def main():
    logger.info(f"🧭 Starting scheduler with TZ={APP_TZ}")
    sched = BlockingScheduler(timezone=APP_TZ)

    # ۱) زمان‌بندی همه DAILY_TASKS: از 20:00 با فاصله 1 دقیقه (به‌جز آن‌هایی که CUSTOM دارند)
    schedule_in_window_with_offsets(
        sched,
        DAILY_TASKS,
        WINDOW_START_HOUR,
        WINDOW_START_MINUTE,
        SPACING_MINUTES,
        window_label="daily",
    )

    # ۲) زمان‌بندی INDICATOR_TASKS:
    # برای شروع اندیکاتورها، دقیقه شروع را بعد از اتمام تقریبیِ DAILY_TASKS بدون CUSTOM می‌گذاریم،
    # تا هم‌پوشانی کمتری داشته باشیم؛ اما همچنان فلسفه‌ی «۲۰ تا ۲۱» حفظ شود.
    non_custom_daily = [n for n, _ in DAILY_TASKS if n not in CUSTOM_SCHEDULE]
    indicators_start_offset = len(non_custom_daily) * SPACING_MINUTES
    ind_start_hour = (WINDOW_START_HOUR + (WINDOW_START_MINUTE + indicators_start_offset) // 60) % 24
    ind_start_minute = (WINDOW_START_MINUTE + indicators_start_offset) % 60

    schedule_in_window_with_offsets(
        sched,
        INDICATOR_TASKS,
        ind_start_hour,
        ind_start_minute,
        SPACING_MINUTES,
        window_label="indicators",
    )

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Scheduler stopped.")


if __name__ == "__main__":
    main()
