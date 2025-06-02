from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime
from typing import List

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


# ✅ لیست کل اشتراک‌ها برای مدیریت
@router.get("/admin/user-subscriptions", response_model=List[UserSubscriptionOut])
def list_user_subscriptions_admin(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles(["admin", "superadmin"]))
):
    return db.query(UserSubscription).order_by(UserSubscription.start_date.desc()).all()


# ✅ افزودن اشتراک برای کاربر خاص
@router.post("/admin/user-subscriptions", response_model=UserSubscriptionOut)
def create_user_subscription_admin(
    data: UserSubscriptionCreateAdmin,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles(["admin", "superadmin"]))
):
    subscription = db.query(Subscription).filter_by(id=data.subscription_id).first()
    if not subscription:
        raise HTTPException(status_code=404, detail="پلن پیدا نشد")

    user = db.query(User).filter_by(id=data.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="کاربر پیدا نشد")

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
    return new_sub


# ✅ ویرایش اشتراک خاص
@router.put("/admin/user-subscriptions/{sub_id}", response_model=UserSubscriptionOut)
def update_user_subscription_admin(
    sub_id: int,
    data: UserSubscriptionUpdateAdmin,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles(["admin", "superadmin"]))
):
    sub = db.query(UserSubscription).filter_by(id=sub_id).first()
    if not sub:
        raise HTTPException(status_code=404, detail="اشتراک پیدا نشد")

    for field, value in data.dict(exclude_unset=True).items():
        setattr(sub, field, value)

    db.commit()
    db.refresh(sub)
    return sub


# ✅ حذف (soft delete) اشتراک کاربر توسط ادمین
@router.delete("/admin/user-subscriptions/{sub_id}")
def delete_user_subscription_admin(
    sub_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_roles(["admin", "superadmin"]))
):
    # پیدا کردن اشتراک
    sub = db.query(models.UserSubscription).filter_by(id=sub_id).first()
    if not sub:
        raise HTTPException(status_code=404, detail="❌ اشتراک پیدا نشد")

    # گرفتن کاربر صاحب اشتراک
    user = db.query(models.User).filter_by(id=sub.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="❌ کاربر مربوطه پیدا نشد")

    # ⛔ جلوگیری از حذف اشتراک ادمین یا سوپر ادمین
    role_names = [role.name for role in user.roles]
    if "admin" in role_names or "superadmin" in role_names:
        raise HTTPException(status_code=403, detail="⛔ نمی‌توان اشتراک ادمین یا سوپر ادمین را حذف کرد")

    # انجام soft delete
    sub.is_active = False
    sub.status = "expired"
    sub.deleted_at = datetime.utcnow()

    db.commit()
    return {"message": "✅ اشتراک با موفقیت غیرفعال شد (soft delete)"}
