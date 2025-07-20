from fastapi import APIRouter, Depends,Request
from sqlalchemy.orm import Session
from backend.db.connection import async_session
from backend.users import models
from backend.users.dependencies import require_roles
import traceback
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from sqlalchemy.ext.asyncio import AsyncSession
from backend.users.routes.auth import get_current_user
from backend.users.models import User
from datetime import datetime
from backend.utils.response import create_response  # خروجی هماهنگ با بقیه APIهات

router = APIRouter()

async def get_db():
    async with async_session() as session:
        yield session

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



