
from fastapi import APIRouter, Request, Depends, HTTPException,Query,status as http_status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from typing import List
from backend.users import models, schemas
from backend.db.connection import async_session
from backend.utils.response import create_response
from backend.users.dependencies import require_permissions
from sqlalchemy.orm import selectinload

router = APIRouter()

# 📦 اتصال به دیتابیس
async def get_db():
    async with async_session() as session:
        yield session

# ✅ ایجاد Permission جدید (فقط سوپرادمین)
@router.post("/admin/permissions")
async def create_permission(
    data: schemas.PermissionCreate,
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("Permission.Create","ALL"))
):


    result = await db.execute(select(models.Permission).where(models.Permission.name == data.name))
    existing = result.scalars().first()
    if existing:
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail="Permission تکراری است",
        )
    permission = models.Permission(name=data.name)
    db.add(permission)
    await db.commit()
    await db.refresh(permission)

    # پاسخ موفقیت
    return create_response(
        status_code=http_status.HTTP_201_CREATED,
        message=f"Permission '{permission.name}' ساخته شد",
        data={"id": permission.id, "name": permission.name},
        status="success",
    )

# ✅ لیست کل Permissionها (برای ادمین و سوپرادمین)
@router.get("/admin/permissions")
async def list_permissions(
        db: AsyncSession = Depends(get_db),
        _: models.User = Depends(require_permissions("Permission.ViewAll","ALL")),
        page: int = Query(1, ge=1),
        size: int = Query(10, enum=[10, 50, 100])
):


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
    role_id: int,
    data: schemas.AssignPermissionInput,  # باید داشته باشه: permission_ids: List[int]
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("Permission.ViewById","ALL"))

):

    result = await db.execute(
        select(models.Role)
        .options(selectinload(models.Role.permissions))  # ← این اضافه می‌کنه
        .where(models.Role.id == role_id)
    )
    role = result.scalars().first()
    if not role:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="نقش پیدا نشد",
        )
    added = 0
    skipped = 0
    not_found: list[int] = []

    for perm_id in data.permission_ids:
        result = await db.execute(select(models.Permission).where(models.Permission.id == perm_id))
        perm = result.scalars().first()
        if not perm:
            not_found.append(perm_id)
            continue
        if perm in role.permissions:
            skipped += 1
            continue
        role.permissions.append(perm)
        added += 1

    await db.commit()
    return create_response(
        status_code=http_status.HTTP_200_OK,
        status="success",
        message=f"{added} دسترسی جدید اضافه شد. {skipped} تکراری بود.",
        data={
            "added": added,
            "skipped": skipped,
            "not_found": not_found,
            "role_id": role.id,
        },
    )

