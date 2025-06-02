from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from backend.db.connection import SessionLocal
from backend.users import models
from backend.users.dependencies import require_roles

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ✅ لیست کاربران (فقط برای ادمین‌ها)
@router.get("/admin/users")
def list_users_for_admin(
    db: Session = Depends(get_db),
    _: models.User = Depends(require_roles(["admin", "superadmin"]))
):
    users = db.query(models.User).all()
    return [
        {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "is_active": user.is_active,
            "roles": [role.name for role in user.roles]
        }
        for user in users
    ]
