from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from backend.db.connection import SessionLocal
from backend.users import models, schemas
from backend.users.dependencies import require_roles
from backend.users.routes.auth import get_current_user

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/seed/superadmin")
def seed_superadmin(db: Session = Depends(get_db)):
    # بررسی وجود نقش
    role = db.query(models.Role).filter(models.Role.name == "superadmin").first()
    if not role:
        role = models.Role(name="superadmin", description="کاربر ریشه با دسترسی کامل")
        db.add(role)
        db.commit()
        db.refresh(role)

    # گرفتن اولین کاربر (یا تغییر به user_id دلخواه)
    user = db.query(models.User).filter(models.User.id == 1).first()
    if not user:
        return {"error": "❌ کاربر با id=1 وجود ندارد"}

    if role in user.roles:
        return {"message": "✅ کاربر قبلاً نقش superadmin دارد"}

    user.roles.append(role)
    db.commit()
    return {"message": "✅ نقش superadmin به کاربر 1 اختصاص یافت"}

# ✅ ساخت نقش جدید
@router.post("/admin/roles", dependencies=[Depends(require_roles(["superadmin"]))])
def create_role(data: schemas.RoleCreate, db: Session = Depends(get_db)):
    existing = db.query(models.Role).filter(models.Role.name == data.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="نقش تکراری است")
    new_role = models.Role(name=data.name, description=data.description)
    db.add(new_role)
    db.commit()
    db.refresh(new_role)
    return {"message": "✅ نقش ساخته شد", "role_id": new_role.id}

# ✅ لیست نقش‌ها
@router.get("/admin/roles", response_model=List[schemas.RoleOut], dependencies=[Depends(require_roles(["superadmin"]))])
def list_roles(db: Session = Depends(get_db)):
    return db.query(models.Role).all()
#----------------------------------------------------------------------------------
# ✅ اختصاص نقش به کاربر
@router.post("/admin/user/{user_id}/assign-role")
def assign_role_to_user(
    user_id: int,
    data: schemas.AssignRoleInput,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    user = db.query(models.User).filter_by(id=user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="کاربر پیدا نشد")
    role = db.query(models.Role).filter_by(id=data.role_id).first()
    if not role:
        raise HTTPException(status_code=404, detail="نقش پیدا نشد")
    if role in user.roles:
        raise HTTPException(status_code=400, detail="این نقش قبلاً به کاربر داده شده")

    user.roles.append(role)
    db.commit()
    return {"message": f"✅ نقش '{role.name}' به کاربر اضافه شد"}
#------------------------------------------------------------------------------------------
#✅ حذف نقش از کاربر
@router.delete("/admin/user/{user_id}/remove-role")
def remove_role_from_user(
    user_id: int,
    data: schemas.RemoveRoleInput,
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["superadmin"]))
):
    user = db.query(models.User).filter_by(id=user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="کاربر پیدا نشد")
    role = db.query(models.Role).filter_by(id=data.role_id).first()
    if not role:
        raise HTTPException(status_code=404, detail="نقش پیدا نشد")
    if role not in user.roles:
        raise HTTPException(status_code=400, detail="این نقش به کاربر داده نشده")

    user.roles.remove(role)
    db.commit()
    return {"message": f"❎ نقش '{role.name}' از کاربر حذف شد"}
