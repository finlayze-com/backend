# backend/utils/logger.py
import logging
from logging.handlers import RotatingFileHandler
import os
import sys

# 🩵 رفع خطای UnicodeEncodeError در ویندوز (برای لاگ فارسی و ایموجی)
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# مسیر مطلق به پوشه logs
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
log_dir = os.path.join(BASE_DIR, "logs")
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "main.log")  # 👈 مسیر فایل لاگ

# تنظیمات لاگر اصلی
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)

# اطمینان از اینکه فقط یکبار تنظیم می‌شود
if not logger.hasHandlers():
    # ✳️ هندلر فایل با Rotation و UTF-8
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    # ✳️ هندلر کنسول با UTF-8 برای ترمینال
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_formatter = logging.Formatter("%(levelname)s | %(message)s")
    console_handler.setFormatter(console_formatter)

    # ✅ اضافه‌کردن فقط یکبار
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
