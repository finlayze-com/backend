from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from backend.users import models, schemas
from backend.users.dependencies import require_roles
from backend.db.connection import async_session


router = APIRouter()

async def get_db():
    async with async_session() as session:
        yield session


# ✅ ایجاد پرمیشن
@router.post("/admin/permissions")
def create_permission(
    data: schemas.PermissionCreate,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    existing = db.query(models.Permission).filter_by(name=data.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Permission تکراری است")

    new_perm = models.Permission(name=data.name, description=data.description)
    db.add(new_perm)
    db.commit()
    db.refresh(new_perm)
    return {"message": f"✅ Permission '{new_perm.name}' ساخته شد"}

# ✅ لیست کل دسترسی‌ها (Permissionها)
@router.get("/admin/permissions", response_model=List[schemas.PermissionOut])
def list_permissions(
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["admin", "superadmin"]))
):
    return db.query(models.Permission).order_by(models.Permission.id).all()

# ✅ اتصال پرمیشن به نقش
@router.post("/admin/roles/{role_id}/assign-permissions")
def assign_permissions_to_role(
    role_id: int,
    data: schemas.AssignPermissionInput,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    role = db.query(models.Role).filter_by(id=role_id).first()
    if not role:
        raise HTTPException(status_code=404, detail="نقش پیدا نشد")

    added = 0
    skipped = 0

    for perm_id in data.permission_ids:
        perm = db.query(models.Permission).filter_by(id=perm_id).first()
        if not perm:
            continue  # skip invalid
        if perm in role.permissions:
            skipped += 1
            continue
        role.permissions.append(perm)
        added += 1

    db.commit()
    return {
        "message": f"✅ {added} دسترسی جدید اضافه شد. {skipped} تکراری بود.",
        "added": added,
        "skipped": skipped
    }
