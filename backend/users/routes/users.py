from fastapi import APIRouter, Depends, Request, Query, HTTPException, Path
from sqlalchemy.dialects.oracle.dictionary import all_users
from sqlalchemy.orm import Session
from backend.db.connection import async_session
from backend.users import models
from backend.users.dependencies import require_roles, require_permissions
import traceback
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from sqlalchemy.ext.asyncio import AsyncSession
from backend.users.models import User
from datetime import datetime
from backend.utils.response import create_response  # خروجی هماهنگ با بقیه APIهات
from backend.utils.pagination import paginate
from sqlalchemy.exc import IntegrityError
from backend.users import models, schemas
from backend.users.routes.auth import get_password_hash
from backend.users.schemas import UserUpdate
from fastapi import status as http_status


router = APIRouter()

async def get_db():
    async with async_session() as session:
        yield session

# ✅ لیست کاربران (فقط برای ادمین‌ها)
@router.get("/admin/users")
async def list_users_for_admin(
    db: AsyncSession  = Depends(get_db),
    _: models.User = Depends(require_permissions("ALL")),
    page: int = Query(1, ge=1),
    size: int = Query(10, enum=[10, 50, 100])  # فقط مقادیر خاص مجاز
):
    stmt = select(models.User).options(joinedload(models.User.roles))
    result = await db.execute(stmt)
    users = result.unique().scalars().all()

    # صفحه‌بندی دستی (چون offset مستقیم روی select async کار نمی‌کنه مگر پیچیده‌تر)
    start = (page - 1) * size
    end = start + size
    paginated_users = users[start:end]

    items = [
        {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "is_active": user.is_active,
            "roles": [role.name for role in user.roles]
        }
        for user in paginated_users
    ]

    return create_response(
        status="success",
        message="لیست کاربران",
        data={
            "items": items,
            "total": len(users),
            "page": page,
            "size": size,
            "pages": (len(users) + size - 1) // size
        }
    )

# 📌 ایجاد کاربر
@router.post("/admin/users")
async def create_user_for_admin(
    payload: schemas.UserCreate,
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("ALL")),  # هماهنگ با منطق بالا
):
    # 1) چک یکتا بودن username/email
    exists_stmt = select(models.User).where(
        (models.User.username == payload.username) |
        (models.User.email == payload.email)
    )
    exists = (await db.execute(exists_stmt)).scalars().first()
    if exists:
        # ⛔️ بگذار هندلرها پاسخ استاندارد بدهند
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail="نام کاربری یا ایمیل تکراری است",
        )

    # helper کوچک برای تبدیل به UpperCase
    def _to_db_user_type(v):
        if v is None:
            return None
        return str(getattr(v, "value", v)).strip().upper()

    # 2) ساخت کاربر با فیلدهای schemas.UserCreate
    user = models.User(
        username=payload.username,
        email=payload.email,
        password_hash=get_password_hash(payload.password),
        # فیلدهای اختیاری — اگر در مدل User موجودند، ست می‌شوند
        phone_number=getattr(payload, "phone_number", None),
        first_name=getattr(payload, "first_name", None),
        last_name=getattr(payload, "last_name", None),
        user_type=_to_db_user_type(getattr(payload, "user_type", None)),  # ← اصلاح شد
        national_code=getattr(payload, "national_code", None),
        company_national_id=getattr(payload, "company_national_id", None),
        economic_code=getattr(payload, "economic_code", None),
        is_active=True,
    )

    db.add(user)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        # ⛔️ فقط raise تا به handle_integrity_error برود
        raise exc

    await db.refresh(user)


    # 3) برای خروجی مشابه لیست، نقش‌ها رو eager-load می‌کنیم
    stmt = (
        select(models.User)
        .options(joinedload(models.User.roles))
        .where(models.User.id == user.id)
    )
    user_full = (await db.execute(stmt)).unique().scalars().first()

    # 4) خروجی هماهنگ با روت لیست کاربران
    return create_response(
        status_code=http_status.HTTP_201_CREATED,
        status="success",
        message="کاربر با موفقیت ایجاد شد",
        data={
            "id": user_full.id,
            "username": user_full.username,
            "email": user_full.email,
            "is_active": user_full.is_active,
            "roles": [r.name for r in getattr(user_full, "roles", [])],
        },
    )


# 📌 دریافت کاربر با ID
@router.get("/admin/users/{user_id}", response_model=schemas.UserOut)
async def get_user_by_id(
    user_id: int = Path(..., ge=1),
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("User.ViewAll")),
    __: models.User = Depends(require_roles("admin", "superadmin"))
):
    stmt = select(User).options(joinedload(User.roles)).where(User.id == user_id)
    user = (await db.execute(stmt)).unique().scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="کاربر یافت نشد")
    return create_response(
        status_code=http_status.HTTP_200_OK,
        status="success",
        message="اطلاعات کاربر با موفقیت دریافت شد",
        data={
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "is_active": user.is_active,
            "roles": [r.name for r in (user.roles or [])],
        },
    )

# 📌 بروزرسانی کاربر
@router.put("/admin/users/{user_id}")
async def update_user_for_admin(
    user_id: int,
    payload: UserUpdate,
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("User.Update","ALL")),  # هماهنگ با منطق بالا
):
    # 1) پیدا کردن کاربر
    stmt = select(models.User).where(models.User.id == user_id)
    user = (await db.execute(stmt)).scalars().first()
    if not user:
        # ⛔️ خطا از مسیر هندلرها
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="کاربر یافت نشد",
        )

    # 2) اگر username/email قرار است تغییر کند، چک یکتا بودن
    new_username = payload.username if payload.username is not None else user.username
    new_email = payload.email if payload.email is not None else user.email

    # 2) اگر username/email قرار است تغییر کند، چک یکتا بودن
    if (payload.username is not None) or (payload.email is not None):
        conflict_stmt = select(models.User).where(
            (models.User.id != user_id) &
            ((models.User.username == new_username) | (models.User.email == new_email))
            )

        conflict = (await db.execute(conflict_stmt)).scalars().first()
        if conflict:
            raise HTTPException(
                status_code=http_status.HTTP_409_CONFLICT,
                detail="نام کاربری یا ایمیل تکراری است",
            )


    # 3) اعمال تغییراتِ ارسال‌شده (فقط فیلدهای موجود)
    if payload.username is not None:
        user.username = payload.username
    if payload.email is not None:
        user.email = payload.email
    if payload.password:
        user.password_hash = get_password_hash(payload.password)
    if payload.phone_number is not None:
        user.phone_number = payload.phone_number
    if payload.first_name is not None:
        user.first_name = payload.first_name
    if payload.last_name is not None:
        user.last_name = payload.last_name
    if payload.user_type is not None:
        user.user_type = payload.user_type
    if payload.national_code is not None:
        user.national_code = payload.national_code
    if payload.company_national_id is not None:
        user.company_national_id = payload.company_national_id
    if payload.economic_code is not None:
        user.economic_code = payload.economic_code
    if payload.is_active is not None:
        user.is_active = payload.is_active

        # 4) ذخیره و هندل استاندارد خطاهای دیتابیس
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        #  بگذار هندلر IntegrityError پاسخ استاندارد بدهد
        raise exc

    await db.refresh(user)


    # 5) برای خروجی یکدست با لیست، نقش‌ها را لود کن
    stmt_out = (
        select(models.User)
        .options(joinedload(models.User.roles))
        .where(models.User.id == user.id)
    )
    user_full = (await db.execute(stmt_out)).unique().scalars().first()

    # 6) خروجی هماهنگ با روت لیست
    return create_response(
        status_code=http_status.HTTP_200_OK,
        status="success",
        message="کاربر با موفقیت ویرایش شد",
        data={
            "id": user_full.id,
            "username": user_full.username,
            "email": user_full.email,
            "is_active": user_full.is_active,
            "roles": [r.name for r in getattr(user_full, "roles", [])],
        },
    )


# 📌 حذف کاربر
@router.delete("/admin/users/{user_id}")
async def delete_user_for_admin(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("User.Delete","ALL")),  # هماهنگ با لیست
):
    # 1) پیدا کردن کاربر
    stmt = select(models.User).where(models.User.id == user_id)
    user = (await db.execute(stmt)).scalars().first()
    if not user:
        # ⛔️ بگذار هندلر 404 پاسخ استاندارد بدهد
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="کاربر یافت نشد",
        )

    # 2) تلاش برای حذف
    try:
        await db.delete(user)
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        # ⛔️ فقط raise تا به handle_integrity_error برسد (و پیام/کد استاندارد بدهد)
        raise exc

        return create_response(
            status="error",
            message="حذف ممکن نیست؛ وابستگی داده‌ای وجود دارد",
            data=str(e.orig) if hasattr(e, "orig") else str(e)
        )

    # 3) پاسخ موفق
    return create_response(
        status_code=http_status.HTTP_200_OK,
        status="success",
        message="کاربر با موفقیت حذف شد",
        data={"id": user_id}
    )