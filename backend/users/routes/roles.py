
from fastapi import APIRouter, Request, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from typing import List
from sqlalchemy.orm import selectinload
from backend.db.connection import async_session
from backend.users import models, schemas
from backend.users.dependencies import require_permissions
from backend.utils.response import create_response
from sqlalchemy import select  # حتما اضافه کن
from fastapi import Query
from backend.users.models import User
from fastapi import status as http_status
from sqlalchemy.exc import IntegrityError


router = APIRouter()

# 📦 اتصال به دیتابیس
async def get_db():
    async with async_session() as session:
        yield session

# ✅ اختصاص نقش سوپرادمین به کاربر 1 (فقط برای seed اولیه)
@router.post("/seed/superadmin")
async def seed_superadmin(db: AsyncSession  = Depends(get_db)):
    # 1. بررسی وجود نقش
    result = await db.execute(select(models.Role).where(models.Role.name == "superadmin"))
    role = result.scalars().first()

    if not role:
        role = models.Role(name="superadmin", description="کاربر ریشه با دسترسی کامل")
        try:
            await db.commit()
        except IntegrityError as exc:
            await db.rollback()
            raise exc
        await db.refresh(role)
        role_created = True

    # 2. بررسی وجود کاربر

    result = await db.execute(
        select(models.User)
        .options(selectinload(models.User.roles))  # 🔁 بارگذاری roles همزمان با user
        .where(models.User.id == 1)
    )
    user = result.scalars().first()

    if not user:
        # ⛔️ بگذار هندلر 404 پاسخ استاندارد بدهد
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="کاربر با id=1 یافت نشد",
        )    # 3. بررسی اینکه نقش دارد یا نه

    # 3) اگر نقش را دارد، خروجی 200 بده؛ وگرنه نقش را اضافه کن و 201 بده
    if role in user.roles:
        return create_response(
            status_code=http_status.HTTP_200_OK,
            status="success",
            message="کاربر قبلاً نقش superadmin دارد",
            data={"user_id": user.id, "role": role.name, "role_created": role_created},
    )


    user.roles.append(role)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise exc

    return create_response(
        status_code=http_status.HTTP_201_CREATED,
        status="success",
        message="نقش superadmin به کاربر 1 اختصاص یافت",
        data={"user_id": user.id, "role": role.name, "role_created": role_created},
    )

# ✅ ساخت نقش جدید (فقط برای سوپرادمین)
@router.post("/admin/roles")
async def create_role(

    data: schemas.RoleCreate,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_permissions("Role.Create","ALL")),

):

    # ✅ بررسی نقش تکراری (با async)
    result = await db.execute(select(models.Role).where(models.Role.name == data.name))
    existing = result.scalars().first()

    if existing:
        # ⛔️ بگذار هندلرها جواب استاندارد بدهند
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail="نقش تکراری است",
        )

    # ✅ ساخت نقش جدید
    new_role = models.Role(name=data.name, description=data.description)
    db.add(new_role)
    # commit با هندل درست IntegrityError (بره به هندلر دیتابیس)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise exc
    await db.refresh(new_role)

    role_data = {
        "id": new_role.id,
        "name": new_role.name,
        "description": new_role.description
    }

    return create_response(
        status_code=http_status.HTTP_201_CREATED,
        status="success",
        message="✅ نقش با موفقیت ساخته شد",
        data={"role": role_data}
    )

# ✅ لیست نقش‌ها (فقط سوپرادمین)
@router.get("/admin/roles")
async def list_roles(
        db: AsyncSession = Depends(get_db),
        _: User = Depends(require_permissions("Role.ViewAll","ALL")),
        page: int = Query(1, ge=1),
        size: int = Query(10, enum=[10, 50, 100]),
):

    result = await db.execute(select(models.Role))
    roles = result.scalars().all()

    # صفحه‌بندی دستی
    start = (page - 1) * size
    end = start + size
    paginated_roles = roles[start:end]

    role_list = [
        {
            "id": role.id,
            "name": role.name,
            "description": role.description
        }
        for role in paginated_roles
    ]


    return create_response(
            status="success",
            message="✅ لیست نقش‌ها با موفقیت دریافت شد",
            data={
                "items": role_list,
                "total": len(roles),
                "page": page,
                "size": size,
                "pages": (len(roles) + size - 1) // size
            }
        )

# ✅ اختصاص نقش به کاربر خاص (فقط سوپرادمین)
@router.post("/admin/user/{user_id}/assign-role")
async def assign_role_to_user(
    user_id: int,
    data: schemas.AssignRoleInput,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_permissions("Role.AssignToUser","ALL")),
):

    result = await db.execute(
        select
        (models.User)
        .options(selectinload(models.User.roles))
        .where(models.User.id == user_id)
    )
    user = result.scalars().first()
    if not user:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="کاربر پیدا نشد",
        )

    result = await db.execute(select(models.Role).where(models.Role.id == data.role_id))
    role = result.scalars().first()
    if not role:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="نقش پیدا نشد",
        )
    if role in user.roles:
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail="این نقش قبلاً به کاربر داده شده",
        )

    user.roles.append(role)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise exc

    return create_response(
        status_code=http_status.HTTP_200_OK,
        status="success",
        message=f"نقش '{role.name}' به کاربر اضافه شد",
        data={"user_id": user.id, "role_id": role.id, "role_name": role.name},
    )

# ✅ حذف نقش از کاربر خاص (فقط سوپرادمین)
@router.delete("/admin/user/{user_id}/remove-role")
async def remove_role_from_user(
    request: Request,
    user_id: int,
    data: schemas.RemoveRoleInput,
    db: AsyncSession = Depends(get_db),
_: User = Depends(require_permissions("Role.AssignToUser","ALL")),

):
    # 🔍 پرینت مقدار role_names از توکن دیکد شده
    print(" request.state.role_names =", request.state.role_names)



    # بارگذاری کاربر با نقش‌هایش
    result = await db.execute(
        select(models.User).options(selectinload(models.User.roles)).where(models.User.id == user_id)
    )
    user = result.scalars().first()
    if not user:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="کاربر پیدا نشد",
        )
    # پیدا کردن نقش
    result = await db.execute(select(models.Role).where(models.Role.id == data.role_id))
    role = result.scalars().first()
    if not role:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="نقش پیدا نشد",
        )
    if role not in user.roles:
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail="این نقش به کاربر داده نشده",
        )
    user.roles.remove(role)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise exc

    return create_response(
        status_code=http_status.HTTP_200_OK,
        status="success",
        message=f"نقش '{role.name}' از کاربر حذف شد",
        data={"user_id": user.id, "role_id": role.id, "role_name": role.name},
    )