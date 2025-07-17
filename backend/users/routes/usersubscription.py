from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import datetime
from typing import List
from backend.utils.response import create_response  # ✅ تابع استانداردسازی خروجی
from backend.users.models import User, UserSubscription, Subscription
from backend.users.schemas import (
    UserSubscriptionOut,
    UserSubscriptionCreateAdmin,
    UserSubscriptionUpdateAdmin
)
from backend.db.connection import SessionLocal
from backend.users.dependencies import require_roles
from backend.users import models

router = APIRouter()


# 📦 اتصال به دیتابیس
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ✅ لیست تمام اشتراک‌های کاربران (برای مدیریت)
@router.get("/admin/user-subscriptions")
def list_user_subscriptions_admin(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles(["admin", "superadmin"]))
):
    subscriptions = db.query(UserSubscription).order_by(UserSubscription.start_date.desc()).all()
    return create_response(
        status="success",
        message="لیست اشتراک‌های کاربران با موفقیت دریافت شد",
        data={"subscriptions": subscriptions}
    )


# ✅ افزودن اشتراک جدید برای کاربر خاص توسط ادمین
@router.post("/admin/user-subscriptions")
def create_user_subscription_admin(
    data: UserSubscriptionCreateAdmin,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles(["admin", "superadmin"]))
):
    # بررسی وجود پلن
    subscription = db.query(Subscription).filter_by(id=data.subscription_id).first()
    if not subscription:
        return create_response(
            status="failed",
            message="پلن پیدا نشد",
            data={"errors": {"subscription_id": ["پلن با این شناسه وجود ندارد."]}}
        )

    # بررسی وجود کاربر
    user = db.query(User).filter_by(id=data.user_id).first()
    if not user:
        return create_response(
            status="failed",
            message="کاربر پیدا نشد",
            data={"errors": {"user_id": ["کاربر با این شناسه وجود ندارد."]}}
        )

    new_sub = UserSubscription(
        user_id=data.user_id,
        subscription_id=data.subscription_id,
        start_date=data.start_date,
        end_date=data.end_date,
        is_active=data.is_active,
        method=data.method,
        status=data.status
    )
    db.add(new_sub)
    db.commit()
    db.refresh(new_sub)

    return create_response(
        status="success",
        message="اشتراک برای کاربر با موفقیت ایجاد شد",
        data={"subscription": new_sub}
    )


# ✅ ویرایش اشتراک کاربر
@router.put("/admin/user-subscriptions/{sub_id}")
def update_user_subscription_admin(
    sub_id: int,
    data: UserSubscriptionUpdateAdmin,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles(["admin", "superadmin"]))
):
    sub = db.query(UserSubscription).filter_by(id=sub_id).first()
    if not sub:
        return create_response(
            status="failed",
            message="اشتراک پیدا نشد",
            data={"errors": {"sub_id": ["هیچ اشتراکی با این شناسه وجود ندارد."]}}
        )

    for field, value in data.dict(exclude_unset=True).items():
        setattr(sub, field, value)

    db.commit()
    db.refresh(sub)

    return create_response(
        status="success",
        message="اشتراک با موفقیت بروزرسانی شد",
        data={"subscription": sub}
    )


# ✅ غیرفعال‌سازی (soft delete) اشتراک کاربر توسط ادمین
@router.delete("/admin/user-subscriptions/{sub_id}")
def delete_user_subscription_admin(
    sub_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_roles(["admin", "superadmin"]))
):
    sub = db.query(models.UserSubscription).filter_by(id=sub_id).first()
    if not sub:
        return create_response(
            status="failed",
            message="❌ اشتراک پیدا نشد",
            data={"errors": {"sub_id": ["اشتراک با این شناسه وجود ندارد."]}}
        )

    user = db.query(models.User).filter_by(id=sub.user_id).first()
    if not user:
        return create_response(
            status="failed",
            message="❌ کاربر مربوطه پیدا نشد",
            data={"errors": {"user_id": ["کاربر مربوط به این اشتراک پیدا نشد."]}}
        )

    # جلوگیری از حذف اشتراک ادمین یا سوپر ادمین
    role_names = [role.name for role in user.roles]
    if "admin" in role_names or "superadmin" in role_names:
        return create_response(
            status="failed",
            message="⛔ نمی‌توان اشتراک ادمین یا سوپر ادمین را حذف کرد",
            data={"errors": {"roles": ["شما مجاز به حذف این نوع اشتراک نیستید."]}}
        )

    # soft delete
    sub.is_active = False
    sub.status = "expired"
    sub.deleted_at = datetime.utcnow()
    db.commit()

    return create_response(
        status="success",
        message="✅ اشتراک با موفقیت غیرفعال شد (soft delete)",
        data={"subscription_id": sub_id}
    )
