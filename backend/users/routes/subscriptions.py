from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import List

from backend.users import models, schemas
from backend.db.connection import SessionLocal
from backend.users.routes.auth import get_current_user
from backend.users.dependencies import require_roles
from backend.utils.response import create_response

router = APIRouter()

# 🔧 Helper
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ✅ خرید پلن (اشتراک)
@router.post("/subscribe")
def subscribe_to_plan(
    data: schemas.UserSubscribeIn,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user)
):
    # ۱. پیدا کردن پلن
    subscription = db.query(models.Subscription).filter(
        models.Subscription.id == data.subscription_id,
        models.Subscription.is_active == True
    ).first()

    if not subscription:
        raise HTTPException(status_code=404, detail="پلن یافت نشد یا غیرفعال است")

    now = datetime.utcnow()

    # ۲. پیدا کردن آخرین تاریخ پایان برای کاربر (حتی اگر پلن‌های مختلف باشن)
    latest_end = db.query(models.UserSubscription).filter(
        models.UserSubscription.user_id == user.id
    ).order_by(models.UserSubscription.end_date.desc()).first()

    # ۳. محاسبه تاریخ شروع اشتراک جدید
    if latest_end and latest_end.end_date > now:
        start_date = latest_end.end_date
    else:
        start_date = now

    end_date = start_date + timedelta(days=subscription.duration_days)

    # ۴. فعال‌سازی فقط اگر زمان شروع رسیده باشد
    is_active = start_date <= now < end_date

    # ۵. ساخت اشتراک
    new_sub = models.UserSubscription(
        user_id=user.id,
        subscription_id=subscription.id,
        start_date=start_date,
        end_date=end_date,
        is_active=is_active,
        method=data.method,
        status="active"
    )
    db.add(new_sub)

    # ۶. اگر نقش مرتبط دارد و هنوز به کاربر ندادیم
    if subscription.role_id:
        role = db.query(models.Role).filter(models.Role.id == subscription.role_id).first()
        if role and role not in user.roles:
            user.roles.append(role)

    # ۷. غیرفعال‌سازی همه اشتراک‌هایی که now در بازه‌شون نیست
    all_subs = db.query(models.UserSubscription).filter(
        models.UserSubscription.user_id == user.id
    ).all()
    for sub in all_subs:
        sub.is_active = sub.start_date <= now < sub.end_date

    db.commit()
    db.refresh(new_sub)

    return {
        "message": "✅ اشتراک با موفقیت ثبت شد",
        "subscription_id": new_sub.id,
        "start_date": start_date,
        "end_date": end_date,
        "will_become_active": is_active
    }
# ✅ اشتراک‌های من
@router.get("/my-subscriptions", response_model=List[schemas.UserSubscriptionOut])
def get_my_subscriptions(
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user)
):
    return db.query(models.UserSubscription)\
        .filter(models.UserSubscription.user_id == user.id)\
        .order_by(models.UserSubscription.start_date.desc())\
        .all()

# ✅ لیست اشتراک‌های فعال
#@router.get("/subscriptions", response_model=List[schemas.SubscriptionOut])
#def list_active_subscriptions(db: Session = Depends(get_db)):
#    return db.query(models.Subscription).filter(models.Subscription.is_active == True).all()

# ✅ دریافت اطلاعات یک پلن خاص برای ویرایش
@router.get("/admin/subscriptions/{subscription_id}", response_model=schemas.SubscriptionOut)
def get_subscription_by_id(
    subscription_id: int,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    sub = db.query(models.Subscription).filter_by(id=subscription_id).first()
    if not sub:
        raise HTTPException(status_code=404, detail="پلن پیدا نشد")
    return sub

# ✅ لیست کامل پلن‌ها برای سوپرادمین با خروجی ساختاریافته
@router.get("/subscriptions")
def get_all_subscriptions(
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    plans = db.query(models.Subscription).order_by(models.Subscription.id).all()

    # تبدیل مدل به dict
    plan_list = [
        {
            "id": plan.id,
            "name": plan.name,
            "name_fa": plan.name_fa,
            "name_en": plan.name_en,
            "duration_days": plan.duration_days,
            "price": plan.price,
            "features": plan.features,
            "role_id": plan.role_id,
            "is_active": plan.is_active,
        }
        for plan in plans
    ]

    return create_response(
        status="success",
        message="✅ لیست پلن‌ها با موفقیت دریافت شد",
        data={"subscriptions": plan_list}
    )

# ✅ ساخت پلن جدید (با خروجی یکنواخت)
@router.post("/admin/subscriptions")
def create_subscription(
    data: schemas.SubscriptionCreate,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    existing = db.query(models.Subscription).filter_by(name=data.name).first()
    if existing:
        return create_response(
            status="failed",
            message="نام پلن تکراری است.",
            data={"errors": {"name": ["پلنی با این نام قبلاً ثبت شده است."]}}
        )

    new_sub = models.Subscription(
        name=data.name,
        name_fa=data.name_fa,
        name_en=data.name_en,
        duration_days=data.duration_days,
        price=data.price,
        features=data.features,
        role_id=data.role_id,
        is_active=True
    )
    db.add(new_sub)
    db.commit()
    db.refresh(new_sub)

    sub_data = {
        "id": new_sub.id,
        "name": new_sub.name,
        "name_fa": new_sub.name_fa,
        "name_en": new_sub.name_en,
        "duration_days": new_sub.duration_days,
        "price": new_sub.price,
        "features": new_sub.features,
        "role_id": new_sub.role_id,
        "is_active": new_sub.is_active,
    }

    return create_response(
        status="success",
        message="پلن جدید با موفقیت ساخته شد.",
        data={"subscription": sub_data}
    )

# ✅ ویرایش پلن
@router.put("/admin/subscriptions/{subscription_id}", response_model=schemas.SubscriptionOut)
def update_subscription(
    subscription_id: int,
    data: schemas.SubscriptionUpdate,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    sub = db.query(models.Subscription).filter_by(id=subscription_id).first()
    if not sub:
        raise HTTPException(status_code=404, detail="پلن پیدا نشد")

    for field, value in data.dict(exclude_unset=True).items():
        setattr(sub, field, value)

    db.commit()
    db.refresh(sub)
    return sub

# ✅ حذف یا غیرفعال‌سازی پلن
@router.delete("/admin/subscriptions/{subscription_id}")
def delete_subscription(
    subscription_id: int,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    sub = db.query(models.Subscription).filter_by(id=subscription_id).first()
    if not sub:
        raise HTTPException(status_code=404, detail="پلن پیدا نشد")

    # ⛔ چک می‌کنیم که آیا کاربران این پلن را دارند یا نه
    related_users = db.query(models.UserSubscription).filter_by(subscription_id=subscription_id).count()
    if related_users > 0:
        raise HTTPException(
                status_code=400,
                detail="❌ این پلن به کاربران اختصاص داده شده است و نمی‌توان آن را حذف کرد."
        )

    sub.is_active = False
    sub.deleted_at = datetime.utcnow()  # ← در صورت وجود این ستون
    db.commit()
    return {"message": "✅ پلن با موفقیت غیرفعال شد (soft deleted)"}