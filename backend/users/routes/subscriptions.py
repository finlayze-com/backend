from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import List

from backend.users import models, schemas
from backend.db.connection import async_session
from backend.users.routes.auth import get_current_user
from backend.users.dependencies import require_roles
from backend.utils.response import create_response

router = APIRouter()

# 🔧 Helper
async def get_db():
    async with async_session() as session:
        yield session


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

    # ۲. پیدا کردن آخرین تاریخ پایان برای کاربر
    latest_end = db.query(models.UserSubscription).filter(
        models.UserSubscription.user_id == user.id
    ).order_by(models.UserSubscription.end_date.desc()).first()

    # ۳. محاسبه تاریخ شروع
    start_date = latest_end.end_date if latest_end and latest_end.end_date > now else now
    end_date = start_date + timedelta(days=subscription.duration_days)
    is_active = start_date <= now < end_date

    # ۴. ساخت اشتراک
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

    # ۵. افزودن نقش در صورت نیاز
    if subscription.role_id:
        role = db.query(models.Role).filter(models.Role.id == subscription.role_id).first()
        if role and role not in user.roles:
            user.roles.append(role)

    # ۶. بروزرسانی is_active همه اشتراک‌ها
    all_subs = db.query(models.UserSubscription).filter(
        models.UserSubscription.user_id == user.id
    ).all()
    for sub in all_subs:
        sub.is_active = sub.start_date <= now < sub.end_date

    db.commit()
    db.refresh(new_sub)

    return {
        "status": "success",
        "message": "✅ اشتراک با موفقیت ثبت شد",
        "data": {
            "subscription": {
                "id": new_sub.id,
                "subscription_id": new_sub.subscription_id,
                "start_date": new_sub.start_date,
                "end_date": new_sub.end_date,
                "is_active": new_sub.is_active,
                "method": new_sub.method,
                "status": new_sub.status
            }
        }
    }


# ✅ اشتراک‌های من
@router.get("/my-subscriptions")
def get_my_subscriptions(
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user)
):
    subscriptions = db.query(models.UserSubscription)\
        .filter(models.UserSubscription.user_id == user.id)\
        .order_by(models.UserSubscription.start_date.desc())\
        .all()

    return {
        "status": "success",
        "message": "لیست اشتراک‌های شما با موفقیت دریافت شد",
        "data": {
            "subscriptions": subscriptions
        }
    }


# ✅ لیست اشتراک‌های فعال
#@router.get("/subscriptions", response_model=List[schemas.SubscriptionOut])
#def list_active_subscriptions(db: Session = Depends(get_db)):
#    return db.query(models.Subscription).filter(models.Subscription.is_active == True).all()

# ✅ دریافت اطلاعات یک پلن خاص با خروجی ساختاریافته
@router.get("/admin/subscriptions/{subscription_id}")
def get_subscription_by_id(
    subscription_id: int,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    sub = db.query(models.Subscription).filter_by(id=subscription_id).first()
    if not sub:
        return create_response(
            status="failed",
            message="پلن یافت نشد",
            data={"errors": {"subscription_id": ["هیچ پلنی با این شناسه وجود ندارد."]}}
        )

    return create_response(
        status="success",
        message="اطلاعات پلن با موفقیت دریافت شد",
        data={"subscription": sub}
    )


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
@router.put("/admin/subscriptions/{subscription_id}")
def update_subscription(
    subscription_id: int,
    data: schemas.SubscriptionUpdate,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    sub = db.query(models.Subscription).filter_by(id=subscription_id).first()
    if not sub:
        return create_response(
            status="failed",
            message="پلن پیدا نشد.",
            data={"errors": {"subscription_id": ["شناسه معتبر نیست."]}}
        )

    # فقط فیلدهای داده‌شده را به‌روزرسانی کن
    for field, value in data.dict(exclude_unset=True).items():
        setattr(sub, field, value)

    db.commit()
    db.refresh(sub)

    # اگر می‌خوای خروجی تمیز باشه (مثلاً بدون role_id و is_active)، اینجا کنترل کن
    sub_data = schemas.SubscriptionOut.from_orm(sub)

    return create_response(
        status="success",
        message="پلن با موفقیت ویرایش شد.",
        data={"subscription": sub_data}
    )


# ✅ حذف یا غیرفعال‌سازی پلن با خروجی ساختاریافته
@router.delete("/admin/subscriptions/{subscription_id}")
def delete_subscription(
    subscription_id: int,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    sub = db.query(models.Subscription).filter_by(id=subscription_id).first()
    if not sub:
        return create_response(
            status="failed",
            message="پلن مورد نظر پیدا نشد",
            data={"errors": {"subscription_id": ["پلن با این شناسه یافت نشد."]}}
        )

    # ⛔ چک می‌کنیم که آیا کاربران این پلن را دارند یا نه
    related_users = db.query(models.UserSubscription).filter_by(subscription_id=subscription_id).count()
    if related_users > 0:
        return create_response(
            status="failed",
            message="حذف امکان‌پذیر نیست",
            data={"errors": {"subscription": ["❌ این پلن به کاربران اختصاص داده شده است."]}}
        )

    sub.is_active = False
    sub.deleted_at = datetime.utcnow()  # ← اگر ستون موجود است
    db.commit()

    return create_response(
        status="success",
        message="✅ پلن با موفقیت غیرفعال شد (soft deleted)",
        data={"subscription_id": subscription_id}
    )
