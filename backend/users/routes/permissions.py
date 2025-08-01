
from fastapi import APIRouter, Request, Depends, HTTPException,Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from typing import List
from backend.users import models, schemas
from backend.db.connection import async_session
from backend.utils.response import create_response

router = APIRouter()

# 📦 اتصال به دیتابیس
async def get_db():
    async with async_session() as session:
        yield session

# ✅ ایجاد Permission جدید (فقط سوپرادمین)
@router.post("/admin/permissions")
async def create_permission(
    request: Request,
    data: schemas.PermissionCreate,
    db: AsyncSession = Depends(get_db)
):
    if "superadmin" not in request.state.role_names:
        raise HTTPException(status_code=403, detail="⛔ دسترسی غیرمجاز")

    result = await db.execute(select(models.Permission).where(models.Permission.name == data.name))
    existing = result.scalars().first()
    if existing:
        raise HTTPException(status_code=400, detail="⚠️ Permission تکراری است")

    permission = models.Permission(name=data.name)
    db.add(permission)
    await db.commit()
    await db.refresh(permission)

    return {"message": f"✅ Permission '{permission.name}' ساخته شد", "id": permission.id}

# ✅ لیست کل Permissionها (برای ادمین و سوپرادمین)
@router.get("/admin/permissions")
async def list_permissions(
        request: Request,
        db: AsyncSession = Depends(get_db),
        page: int = Query(1, ge=1),
        size: int = Query(10, enum=[10, 50, 100])
):
    if "admin" not in request.state.role_names and "superadmin" not in request.state.role_names:
        raise HTTPException(status_code=403, detail="⛔ دسترسی غیرمجاز")

    result = await db.execute(select(models.Permission).order_by(models.Permission.id))
    permissions = result.scalars().all()

    # صفحه‌بندی دستی
    start = (page - 1) * size
    end = start + size
    paginated_permissions = permissions[start:end]

    items = [
        {
            "id": perm.id,
            "name": perm.name,
            "description": perm.description,
            "created_at": perm.created_at,
            "updated_at": perm.updated_at,
            "deleted_at": perm.deleted_at,
        }
        for perm in paginated_permissions
    ]

    return create_response(
        status="success",
        message="✅ لیست دسترسی‌ها با موفقیت دریافت شد",
        data={
            "items": items,
            "total": len(permissions),
            "page": page,
            "size": size,
            "pages": (len(permissions) + size - 1) // size
        }
    )

# ✅ اتصال چند پرمیشن به یک نقش خاص (فقط سوپرادمین)
@router.post("/admin/roles/{role_id}/assign-permissions")
async def assign_permissions_to_role(
    request: Request,
    role_id: int,
    data: schemas.AssignPermissionInput,  # باید داشته باشه: permission_ids: List[int]
    db: AsyncSession = Depends(get_db)
):
    if "superadmin" not in request.state.role_names:
        raise HTTPException(status_code=403, detail="⛔ دسترسی غیرمجاز")

    result = await db.execute(select(models.Role).where(models.Role.id == role_id))
    role = result.scalars().first()
    if not role:
        raise HTTPException(status_code=404, detail="نقش پیدا نشد")

    added = 0
    skipped = 0

    for perm_id in data.permission_ids:
        result = await db.execute(select(models.Permission).where(models.Permission.id == perm_id))
        perm = result.scalars().first()
        if not perm:
            continue
        if perm in role.permissions:
            skipped += 1
            continue
        role.permissions.append(perm)
        added += 1

    await db.commit()
    return {
        "message": f"✅ {added} دسترسی جدید اضافه شد. {skipped} تکراری بود.",
        "added": added,
        "skipped": skipped
    }

