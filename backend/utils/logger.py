import logging
from logging.handlers import RotatingFileHandler
import os

# مسیر مطلق به پوشه logs
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
log_dir = os.path.join(BASE_DIR, "logs")
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "main.log")  # 👈 تغییر از app.log به main.log

# تنظیمات لاگر اصلی
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)

# هندلر فایل با Rotation (مثلاً هر فایل 5MB، حداکثر 3 نسخه)
if not logger.hasHandlers():
    file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
    file_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    file_handler.setFormatter(file_formatter)


# هندلر ترمینال (اختیاری)
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter("%(levelname)s | %(message)s")
console_handler.setFormatter(console_formatter)

# اضافه‌کردن هندلرها
logger.addHandler(file_handler)
logger.addHandler(console_handler)
