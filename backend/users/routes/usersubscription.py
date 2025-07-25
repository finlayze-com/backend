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
from backend.db.connection import async_session
from backend.users.dependencies import require_roles
from backend.users import models
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from backend.users.models import UserSubscription
from sqlalchemy.orm import selectinload
from backend.utils.logger import logger

router = APIRouter()


# 📦 اتصال به دیتابیس
async def get_db():
    async with async_session() as session:
        yield session



# ✅ لیست تمام اشتراک‌های کاربران (برای مدیریت)
@router.get("/admin/user-subscriptions")
async def list_user_subscriptions_admin(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_roles(["admin", "superadmin"]))
):
    # 👇 اینجا دقیقاً جای درستشه
    print("🔔 وارد تابع list_user_subscriptions_admin شدیم")
    logger.info("✅ ورود به روت لیست اشتراک‌ها")

    try:
        result = await db.execute(
            select(UserSubscription)
            .options(selectinload(UserSubscription.subscription))
            .order_by(UserSubscription.start_date.desc())
        )
        subscriptions = result.scalars().all()

        subscription_out = [UserSubscriptionOut.from_orm(sub) for sub in subscriptions]

        logger.info(f"📦 تعداد اشتراک یافت‌شده: {len(subscription_out)}")

        return create_response(
            status="success",
            message="لیست اشتراک‌های کاربران با موفقیت دریافت شد",
            data={"subscriptions": subscription_out}
        )

    except Exception as e:
        logger.error("❌ خطا در اجرای کوئری لیست اشتراک‌ها", exc_info=True)
        return create_response(
            status="failed",
            message="خطا در دریافت لیست اشتراک‌ها",
            data={"error": str(e)}
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
