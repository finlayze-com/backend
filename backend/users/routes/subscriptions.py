
from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from backend.users import models, schemas
from backend.db.connection import async_session
from backend.users.dependencies import require_permissions, get_subscription_dependencies
from backend.utils.response import create_response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select,func
from fastapi import Query


router = APIRouter()

# 🔧 Helper
async def get_db():
    async with async_session() as session:
        yield session


# ✅ خرید پلن (اشتراک)
# ✅ روت خرید اشتراک جدید توسط کاربر (نیاز به لاگین دارد)
@router.post("/subscribe")
async def subscribe_to_plan(
    request: Request,
    data: schemas.UserSubscribeIn,
    db: AsyncSession = Depends(get_db)
):
    # 🔐 گرفتن اطلاعات کاربر از middleware
    user = request.state.user

    result = await db.execute(
        select(models.Subscription).where(
            models.Subscription.id == data.subscription_id,
            models.Subscription.is_active == True
        )
    )
    subscription = result.scalars().first()

    if not subscription:
        return create_response(
            status="failed",
            message="پلن یافت نشد یا غیرفعال است",
            data={"subscription_id": ["پلن موجود نیست یا غیرفعال است"]}
        )

    now = datetime.utcnow()
    # ✅ آخرین اشتراک کاربر
    result = await db.execute(
        select(models.UserSubscription).where(
            models.UserSubscription.user_id == user.id
        ).order_by(models.UserSubscription.end_date.desc())
    )
    latest_end = result.scalars().first()

    start_date = latest_end.end_date if latest_end and latest_end.end_date > now else now
    end_date = start_date + timedelta(days=subscription.duration_days)
    is_active = start_date <= now < end_date

    # ✅ ساخت اشتراک جدید
    new_sub = models.UserSubscription(
        user_id=user.id,
        subscription_id=subscription.id,
        start_date = start_date,  # ← تبدیل به str
        end_date = end_date,  # ← تبدیل به str
        is_active=is_active,
        method=data.method,
        status="active"
    )
    db.add(new_sub)
# ✅ اضافه کردن نقش
    if subscription.role_id:
        result = await db.execute(
            select(models.Role).where(models.Role.id == subscription.role_id)
        )
        role = result.scalars().first()
        if role and role not in user.roles:
            user.roles.append(role)

    # ✅ بروزرسانی وضعیت فعال بودن همه اشتراک‌های کاربر
    result = await db.execute(
        select(models.UserSubscription).where(
            models.UserSubscription.user_id == user.id
        )
    )
    all_subs = result.scalars().all()
    for sub in all_subs:
        sub.is_active = sub.start_date <= now < sub.end_date

    await db.commit()
    await db.refresh(new_sub)

    return create_response(
        status="success",
        message="✅ اشتراک با موفقیت ثبت شد",
        data={"subscription": {
            "id": new_sub.id,
            "subscription_id": new_sub.subscription_id,
            "start_date": new_sub.start_date.isoformat(),
            "end_date": new_sub.end_date.isoformat(),
            "is_active": new_sub.is_active,
            "method": new_sub.method,
            "status": new_sub.status
        }}
    )

# ✅ روت گرفتن لیست اشتراک‌های فعال کاربر جاری
@router.get("/my-subscriptions")
async def get_my_subscriptions(request: Request, db: AsyncSession = Depends(get_db)):
        # 🔐 گرفتن اطلاعات کاربر از middleware
        user = request.state.user
        result = await db.execute(
            select(models.UserSubscription).where(
                models.UserSubscription.user_id == user.id
            ).order_by(models.UserSubscription.end_date.desc())
        )
        user_subs = result.scalars().all()

        # تبدیل datetime به isoformat برای JSON
        data = [{
        "id": sub.id,
        "subscription_id": sub.subscription_id,
        "start_date": sub.start_date.isoformat(),
        "end_date": sub.end_date.isoformat(),
        "is_active": sub.is_active,
        "method": sub.method,
        "status": sub.status
    } for sub in user_subs]

        return create_response(
            status="success",
            message="✅ اشتراک‌های کاربر دریافت شد",
            data={"subscriptions": data}
        )

# ✅ دریافت اطلاعات یک پلن خاص
# ✅ گرفتن اطلاعات یک پلن خاص (مخصوص سوپرادمین)
@router.get("/admin/subscriptions/{subscription_id}")
async def get_subscription_by_id(
    subscription_id: int,
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("Subscription.ViewById","ALL"))
):

    # 🧠 استفاده از select به جای query
    result = await db.execute(
        select(models.Subscription).where(models.Subscription.id == subscription_id)
    )
    sub = result.scalar_one_or_none()

    if not sub:
        return create_response(
            status="failed",
            message="پلن یافت نشد",
            data={"errors": {"subscription_id": ["هیچ پلنی با این شناسه وجود ندارد."]}}
        )

    return create_response(
        status="success",
        message="اطلاعات پلن با موفقیت دریافت شد",
        data={"subscription": {
            "id": sub.id,
            "name": sub.name,
            "name_fa": sub.name_fa,
            "name_en": sub.name_en,
            "duration_days": sub.duration_days,
            "price": sub.price,
            "features": sub.features,
            "role_id": sub.role_id,
            "is_active": sub.is_active
        }}
    )

# ✅ لیست کامل پلن‌ها
# ✅ گرفتن لیست کامل پلن‌ها (مخصوص سوپرادمین)
@router.get("/admin/subscriptions")
async def get_all_subscriptions(
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("Subscription.ViewAll","ALL")),
    page: int = Query(1, ge=1),
    size: int = Query(10, enum=[10, 50, 100])
):

    try:
        result = await db.execute(select(models.Subscription).order_by(models.Subscription.id))
        plans = result.scalars().all()

        # صفحه‌بندی دستی
        start = (page - 1) * size
        end = start + size
        paginated = plans[start:end]

        plan_list = [{
            "id": plan.id,
            "name": plan.name,
            "name_fa": plan.name_fa,
            "name_en": plan.name_en,
            "duration_days": plan.duration_days,
            "price": plan.price,
            "features": plan.features,
            "role_id": plan.role_id,
            "is_active": plan.is_active,
        } for plan in paginated  ]

    except Exception as e:
        # اگر مشکلی در دیتابیس بود
        return create_response(
            status="failed",
            message="خطا در بازیابی اطلاعات پلن‌ها",
            data={"error": str(e)}
        )

    return create_response(
        status="success",
        message="✅ لیست پلن‌ها با موفقیت دریافت شد",
        data={
            "items": plan_list,
            "total": len(plans),
            "page": page,
            "size": size,
            "pages": (len(plans) + size - 1) // size
        }
    )

# ✅ لیست کامل پلن‌ها
# ✅ گرفتن لیست کامل پلن‌ها (مخصوص سوپرادمین)
@router.get("/admin/subscriptionswithoutpermisshion")
async def get_all_subscriptions(
    db: AsyncSession = Depends(get_db),
    page: int = Query(1, ge=1),
    size: int = Query(10, enum=[10, 50, 100])
):

    try:
        result = await db.execute(select(models.Subscription).order_by(models.Subscription.id))
        plans = result.scalars().all()

        # صفحه‌بندی دستی
        start = (page - 1) * size
        end = start + size
        paginated = plans[start:end]

        plan_list = [{
            "id": plan.id,
            "name": plan.name,
            "name_fa": plan.name_fa,
            "name_en": plan.name_en,
            "duration_days": plan.duration_days,
            "price": plan.price,
            "features": plan.features,
            "role_id": plan.role_id,
            "is_active": plan.is_active,
        } for plan in paginated  ]

    except Exception as e:
        # اگر مشکلی در دیتابیس بود
        return create_response(
            status="failed",
            message="خطا در بازیابی اطلاعات پلن‌ها",
            data={"error": str(e)}
        )

    return create_response(
        status="success",
        message="✅ لیست پلن‌ها با موفقیت دریافت شد",
        data={
            "items": plan_list,
            "total": len(plans),
            "page": page,
            "size": size,
            "pages": (len(plans) + size - 1) // size
        }
    )



# ✅ ساخت پلن جدید
# ✅ ایجاد یک پلن جدید (مخصوص سوپرادمین)
@router.post("/admin/subscriptions")
async def create_subscription(
    data: schemas.SubscriptionCreate,
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("Subscription.Create","ALL"))
):

        # بررسی تکراری بودن نام پلن
    result = await db.execute(select(models.Subscription).where(models.Subscription.name == data.name))
    existing = result.scalars().first()

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
    await db.commit()
    await db.refresh(new_sub)

    return create_response(
        status="success",
        message="پلن جدید با موفقیت ساخته شد.",
        data={"subscription": {
            "id": new_sub.id,
            "name": new_sub.name,
            "name_fa": new_sub.name_fa,
            "name_en": new_sub.name_en,
            "duration_days": new_sub.duration_days,
            "price": new_sub.price,
            "features": new_sub.features,
            "role_id": new_sub.role_id,
            "is_active": new_sub.is_active,
        }}
    )


# ✅ ویرایش پلن
# ✅ ویرایش یک پلن (مخصوص سوپرادمین)
@router.put("/admin/subscriptions/{subscription_id}")
async def update_subscription(
    subscription_id: int,
    data: schemas.SubscriptionUpdate,
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("Subscription.Create","ALL"))
):

    result = await db.execute(select(models.Subscription).filter_by(id=subscription_id))
    sub = result.scalar_one_or_none()

    if not sub:
        return create_response(
            status="failed",
            message="پلن پیدا نشد.",
            data={"errors": {"subscription_id": ["شناسه معتبر نیست."]}}
        )

    for field, value in data.dict(exclude_unset=True).items():
        setattr(sub, field, value)

    await db.commit()
    await db.refresh(sub)

    sub_data = schemas.SubscriptionOut.model_validate(sub, from_attributes=True)

    return create_response(
        status="success",
        message="پلن با موفقیت ویرایش شد.",
        data={"subscription": sub_data}
    )


# ✅ حذف یا غیرفعال‌سازی پلن
# ✅ غیرفعال‌سازی یا حذف منطقی یک پلن (مخصوص سوپرادمین)
@router.delete("/admin/subscriptions/{subscription_id}")
async def delete_subscription(
    subscription_id: int,
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("Subscription.Delete"))
):

    result = await db.execute(select(models.Subscription).where(models.Subscription.id == subscription_id))
    sub = result.scalars().first()

    if not sub:
        return create_response(
            status="failed",
            message="پلن مورد نظر پیدا نشد",
            data={"errors": {"subscription_id": ["پلن با این شناسه یافت نشد."]}}
        )

        # ✅ بررسی وجود داده در سایر جداول مرتبط با subscription_id
    violating_tables = await get_subscription_dependencies(subscription_id, db)

    if violating_tables:
        return create_response(
            status="failed",
            message="❌ امکان حذف وجود ندارد. این پلن در جدول‌های زیر استفاده شده است.",
            data={"tables": violating_tables}
        )

    sub.is_active = False
    sub.deleted_at = datetime.utcnow()

    await db.commit()

    return create_response(
        status="success",
        message="✅ پلن با موفقیت غیرفعال شد (soft deleted)",
        data={"subscription_id": subscription_id}
    )

