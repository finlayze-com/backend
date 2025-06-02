from datetime import datetime
from sqlalchemy.orm import Session
from backend.db.connection import SessionLocal
from backend.users import models

DEFAULT_ROLE_NAME = "user"

def deactivate_expired_subscriptions():
    db: Session = SessionLocal()
    try:
        now = datetime.utcnow()

        # 1️⃣ غیرفعال‌سازی اشتراک‌های منقضی‌شده
        expired = db.query(models.UserSubscription).filter(
            models.UserSubscription.end_date < now,
            models.UserSubscription.is_active == True
        ).all()

        for sub in expired:
            sub.is_active = False

        # 2️⃣ بررسی برای فعال‌سازی اشتراک بعدی (اگر موجود بود)
        users = db.query(models.User).all()
        for user in users:
            next_sub = db.query(models.UserSubscription).filter(
                models.UserSubscription.user_id == user.id,
                models.UserSubscription.start_date <= now,
                models.UserSubscription.is_active == False
            ).order_by(models.UserSubscription.start_date).first()

            if next_sub:
                next_sub.is_active = True

                # اعمال نقش پلن جدید
                if next_sub.subscription and next_sub.subscription.role_id:
                    role = db.query(models.Role).filter(models.Role.id == next_sub.subscription.role_id).first()
                    if role:
                        user.roles = [role]
                else:
                    # نقش پیش‌فرض اگر پلن نقشی نداشت
                    default_role = db.query(models.Role).filter(models.Role.name == DEFAULT_ROLE_NAME).first()
                    if default_role:
                        user.roles = [default_role]

        db.commit()
        print(f"✅ بررسی و تغییر وضعیت اشتراک‌ها انجام شد.")

    except Exception as e:
        print("❌ خطا در بررسی اشتراک‌ها:", str(e))
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    deactivate_expired_subscriptions()
