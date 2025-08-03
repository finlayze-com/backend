
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
        db.add(role)
        await db.commit()
        await db.refresh(role)

    # 2. بررسی وجود کاربر

    result = await db.execute(
        select(models.User)
        .options(selectinload(models.User.roles))  # 🔁 بارگذاری roles همزمان با user
        .where(models.User.id == 1)
    )
    user = result.scalars().first()

    if not user:
        return {"error": "❌ کاربر با id=1 وجود ندارد"}
    # 3. بررسی اینکه نقش دارد یا نه
    if role in user.roles:
        return {"message": "✅ کاربر قبلاً نقش superadmin دارد"}

    user.roles.append(role)
    await db.commit()
    return {"message": "✅ نقش superadmin به کاربر 1 اختصاص یافت"}

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
        return create_response(
            status="failed",
            message="نقش تکراری است",
            data={"errors": {"name": ["نقش با این نام قبلاً ساخته شده است."]}}
        )

    # ✅ ساخت نقش جدید
    new_role = models.Role(name=data.name, description=data.description)
    db.add(new_role)
    await db.commit()
    await db.refresh(new_role)

    role_data = {
        "id": new_role.id,
        "name": new_role.name,
        "description": new_role.description
    }

    return create_response(
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
        raise HTTPException(status_code=404, detail="کاربر پیدا نشد")

    result = await db.execute(select(models.Role).where(models.Role.id == data.role_id))
    role = result.scalars().first()
    if not role:
        raise HTTPException(status_code=404, detail="نقش پیدا نشد")

    if role in user.roles:
        raise HTTPException(status_code=400, detail="این نقش قبلاً به کاربر داده شده")

    user.roles.append(role)
    await db.commit()

    return {"message": f"✅ نقش '{role.name}' به کاربر اضافه شد"}


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
        raise HTTPException(status_code=404, detail="کاربر پیدا نشد")

    # پیدا کردن نقش
    result = await db.execute(select(models.Role).where(models.Role.id == data.role_id))
    role = result.scalars().first()
    if not role:
        raise HTTPException(status_code=404, detail="نقش پیدا نشد")

    if role not in user.roles:
        raise HTTPException(status_code=400, detail="این نقش به کاربر داده نشده")

    user.roles.remove(role)
    await db.commit()

    return {
        "status": "success",
        "message": f"❎ نقش '{role.name}' از کاربر حذف شد",
        "data": {}
    }