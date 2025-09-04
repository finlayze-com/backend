
from fastapi import APIRouter, Request, Depends, HTTPException,Query,status as http_status
from sqlalchemy import select,func
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


# ---------- Serializers ----------
def serialize_permission(p: models.Permission) -> dict:
    return {
        "id": p.id,
        "name": getattr(p, "name", None),
    }

def serialize_role(r: models.Role) -> dict:
    perms = list(r.permissions or [])
    return {
        "id": r.id,
        "name": getattr(r, "name", None),
        "description": getattr(r, "description", None),
        "permissions": [serialize_permission(p) for p in perms],

    }

# ---------- Helpers ----------
def clamp(val: int, lo: int, hi: int) -> int:
    return max(lo, min(val, hi))

def make_pagination_meta(total: int, page: int, size: int) -> dict:
    total_pages = (total + size - 1) // size if size > 0 else 0
    return {
        "total": total,
        "page": page,
        "size": size,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
    }

# ---------- Route ----------
@router.get("/admin/roles-permissions")
async def list_roles_and_permissions(
    # Pagination for roles
    roles_page: int = Query(1, ge=1, description="شماره صفحه رول‌ها (>=1)"),
    roles_size: int = Query(20, ge=1, le=200, description="تعداد اقلام در هر صفحهٔ رول‌ها (1..200)"),
    # Pagination for permissions
    perms_page: int = Query(1, ge=1, description="شماره صفحه دسترسی‌ها (>=1)"),
    perms_size: int = Query(50, ge=1, le=500, description="تعداد اقلام در هر صفحهٔ دسترسی‌ها (1..500)"),
    db: AsyncSession = Depends(get_db),
    _: models.User = Depends(require_permissions("Role.ViewAll", "Permission.ViewAll", "ALL")),
):
    # (اختیاری) قفل کردن به بازه‌های منطقی اگر پارامترها بیرون از یوآرال Set شده باشند
    roles_page = clamp(roles_page, 1, 10_000_000)
    roles_size = clamp(roles_size, 1, 200)
    perms_page = clamp(perms_page, 1, 10_000_000)
    perms_size = clamp(perms_size, 1, 500)

    # ---------- Roles (with permissions selectinload) ----------
    # Count
    roles_count_stmt = select(func.count(models.Role.id))
    roles_total = (await db.execute(roles_count_stmt)).scalar_one()

    # Page slice
    roles_stmt = (
        select(models.Role)
        .options(selectinload(models.Role.permissions))
        .order_by(models.Role.id.asc())
        .offset((roles_page - 1) * roles_size)
        .limit(roles_size)
    )
    roles = (await db.execute(roles_stmt)).scalars().all()
    roles_items = [serialize_role(r) for r in roles]
    roles_meta = make_pagination_meta(roles_total, roles_page, roles_size)

    # ---------- Permissions ----------
    # Count
    perms_count_stmt = select(func.count(models.Permission.id))
    perms_total = (await db.execute(perms_count_stmt)).scalar_one()

    # Page slice
    perms_stmt = (
        select(models.Permission)
        .order_by(models.Permission.id.asc())
        .offset((perms_page - 1) * perms_size)
        .limit(perms_size)
    )
    permissions = (await db.execute(perms_stmt)).scalars().all()
    permissions_items = [serialize_permission(p) for p in permissions]
    perms_meta = make_pagination_meta(perms_total, perms_page, perms_size)

    # ---------- Response ----------
    data = {
        "roles": {
            "items": roles_items,
            "meta": roles_meta,
        },
        "permissions": {
            "items": permissions_items,
            "meta": perms_meta,
        },
    }

    return create_response(
        status="success",
        status_code=http_status.HTTP_200_OK,
        message="فهرست نقش‌ها و دسترسی‌ها با موفقیت بازیابی شد",
        data=data,
    )
