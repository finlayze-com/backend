from datetime import datetime
from sqlalchemy.orm import Session
from backend.db.connection import SessionLocal
from backend.users import models

DEFAULT_ROLE_NAME = "user"


def deactivate_expired_subscriptions():
    db: Session = SessionLocal()
    try:
        now = datetime.utcnow()

        # 1️⃣ غیرفعال کردن اشتراک‌هایی که زمان‌شان تمام شده و هنوز فعال هستند
        expired = db.query(models.UserSubscription).filter(
            models.UserSubscription.end_date < now,
            models.UserSubscription.is_active == True
        ).all()

        for sub in expired:
            sub.is_active = False

            # 2️⃣ نقش کاربر را به نقش پیش‌فرض تغییر بده
            user = db.query(models.User).filter(models.User.id == sub.user_id).first()
            default_role = db.query(models.Role).filter(models.Role.name == DEFAULT_ROLE_NAME).first()
            if default_role:
                user.roles = [default_role]

        db.commit()
        print(f"✅ بررسی اشتراک‌ها انجام شد. {len(expired)} اشتراک منقضی شد.")

    except Exception as e:
        print("❌ خطا در بررسی اشتراک‌ها:", str(e))
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    deactivate_expired_subscriptions()
