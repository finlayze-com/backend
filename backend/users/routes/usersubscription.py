
from fastapi import APIRouter, Request, Depends,Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import select
from datetime import datetime

from backend.users.dependencies import require_permissions
from backend.utils.response import create_response
from backend.users.models import User, UserSubscription, Subscription
from backend.users.schemas import (
    UserSubscriptionOut,
    UserSubscriptionCreateAdmin,
    UserSubscriptionUpdateAdmin
)
from sqlalchemy.orm import joinedload

from backend.db.connection import async_session
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
    _: User = Depends(require_permissions("UserSubscription.ViewAll")),
    page: int = Query(1, ge=1),
    size: int = Query(10, enum=[10, 50, 100])
):

    logger.info("✅ ورود به روت لیست اشتراک‌ها")
    try:
        result = await db.execute(
            select(UserSubscription)
            .options(selectinload(UserSubscription.subscription))
            .order_by(UserSubscription.start_date.desc())
        )
        subscriptions = result.scalars().all()
        # صفحه‌بندی دستی
        start = (page - 1) * size
        end = start + size
        paginated = subscriptions[start:end]


        subscription_out = [UserSubscriptionOut.from_orm(sub).model_dump(mode="json") for sub in paginated]

        logger.info(f"📦 تعداد اشتراک یافت‌شده: {len(subscription_out)}")
        return create_response(
            status="success",
            message="لیست اشتراک‌های کاربران با موفقیت دریافت شد",
            data={
                "items": subscription_out,
                "total": len(subscriptions),
                "page": page,
                "size": size,
                "pages": (len(subscriptions) + size - 1) // size
            })
    except Exception as e:
        logger.error("❌ خطا در اجرای کوئری لیست اشتراک‌ها", exc_info=True)
        return create_response(
            status="failed",
            message="خطا در دریافت لیست اشتراک‌ها",
            data={"error": str(e)}
        )

# ✅ افزودن اشتراک جدید برای کاربر خاص توسط ادمین
@router.post("/admin/user-subscriptions")
async def create_user_subscription_admin(
    data: UserSubscriptionCreateAdmin,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_permissions("UserSubscription.Create","ALL"))
):

    subscription = await db.get(Subscription, data.subscription_id)
    if not subscription:
        return create_response(
            status="failed",
            message="پلن پیدا نشد",
            data={"errors": {"subscription_id": ["پلن با این شناسه وجود ندارد."]}}
        )

    user = await db.get(User, data.user_id)
    if not user:
        return create_response(
            status="failed",
            message="کاربر پیدا نشد",
            data={"errors": {"user_id": ["کاربر با این شناسه وجود ندارد."]}}
        )

    # --- این بخش قبلاً اشتباهاً داخل if not user بود ---
    end_date = data.end_date
    if end_date is None:
        duration = getattr(subscription, "duration_days", None)
        if not isinstance(duration, int) or duration <= 0:
            return create_response(
                status="failed",
                message="مدت اشتراک پلن نامعتبر است",
                data={"errors": {"duration_days": ["duration_days پلن باید عدد مثبت باشد."]}}
            )
        end_date = data.start_date + timedelta(days=duration)

    if end_date <= data.start_date:
        return create_response(
            status="failed",
            message="end_date باید بعد از start_date باشد",
            data={"errors": {"end_date": ["end_date معتبر نیست."]}}
        )
    # --- پایان اصلاح ---

    new_sub = UserSubscription(
        user_id=data.user_id,
        subscription_id=data.subscription_id,
        start_date=data.start_date,
        end_date=end_date,
        is_active=data.is_active,
        method=data.method,
        status=data.status
    )
    db.add(new_sub)
    await db.commit()
    await db.refresh(new_sub)

    return create_response(
        status="success",
        message="اشتراک برای کاربر با موفقیت ایجاد شد",
        data={
            "subscription": {
                "id": new_sub.id,
                "subscription_id": new_sub.subscription_id,
                "start_date": new_sub.start_date.isoformat(),
                "end_date": new_sub.end_date.isoformat(),
                "is_active": new_sub.is_active,
                "method": new_sub.method,
                "status": new_sub.status,
            }
        },
    )

# ✅ ویرایش اشتراک کاربر
@router.put("/admin/user-subscriptions/{sub_id}")
async def update_user_subscription_admin(
        sub_id: int,
        data: UserSubscriptionUpdateAdmin,
        db: AsyncSession = Depends(get_db),
        _: User = Depends(require_permissions("UserSubscription.Update","ALL"))
):

    result = await db.execute(select(UserSubscription).where(UserSubscription.id == sub_id))
    sub = result.scalar_one_or_none()
    if not sub:
        return create_response(
            status="failed",
            message="اشتراک پیدا نشد",
            data={"errors": {"sub_id": ["هیچ اشتراکی با این شناسه وجود ندارد."]}}
        )

    for field, value in data.dict(exclude_unset=True).items():
        setattr(sub, field, value)

    await db.commit()
    await db.refresh(sub)

    return create_response(
        status="success",
        message="اشتراک با موفقیت بروزرسانی شد",
        data={"subscription": {
            "id": sub.id,
            "subscription_id": sub.subscription_id,
            "start_date": sub.start_date.isoformat() if sub.start_date else None,
            "end_date": sub.end_date.isoformat() if sub.start_date else None,
            "is_active": sub.is_active,
            "method": sub.method,
            "status": sub.status
        }}
    )

# ✅ غیرفعال‌سازی (soft delete) اشتراک کاربر توسط ادمین
@router.delete("/admin/user-subscriptions/{sub_id}")
async def delete_user_subscription_admin(
    sub_id: int,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_permissions("UserSubscription.Delete"))
):

    result = await db.execute(select(UserSubscription).where(UserSubscription.id == sub_id))
    sub = result.scalar_one_or_none()
    if not sub:
        return create_response(
            status="failed",
            message="❌ اشتراک پیدا نشد",
            data={"errors": {"sub_id": ["اشتراک با این شناسه وجود ندارد."]}}
        )

    result_user = await db.execute(
        select(User).options(
            joinedload(User.roles),
            joinedload(User.subscriptions).joinedload(UserSubscription.subscription)
        ).where(User.id == sub.user_id)
    )
    user = result_user.unique().scalar_one_or_none()
    if not user:
        return create_response(
            status="failed",
            message="❌ کاربر مربوطه پیدا نشد",
            data={"errors": {"user_id": ["کاربر مربوط به این اشتراک پیدا نشد."]}}
        )

    role_names = [role.name for role in user.roles]
    if "admin" in role_names or "superadmin" in role_names:
        return create_response(
            status="failed",
            message="⛔ نمی‌توان اشتراک ادمین یا سوپر ادمین را حذف کرد",
            data={"errors": {"roles": ["شما مجاز به حذف این نوع اشتراک نیستید."]}}
        )

    try:
        sub.is_active = False
        sub.status = "expired"
        sub.deleted_at = datetime.utcnow()
        await db.commit()
        await db.refresh(sub)

        return create_response(
            status="success",
            message="✅ اشتراک با موفقیت غیرفعال شد (soft delete)",
            data={
                "subscription_id": sub.id,
                "user_id": sub.user_id,
                "status": sub.status,
                "is_active": sub.is_active,
                "deleted_at": sub.deleted_at.isoformat()
            }
        )
    except Exception as e:
        await db.rollback()
        return create_response(
            status="failed",
            message="❌ خطا در غیرفعال‌سازی اشتراک",
            data={"error": str(e)}
        )
