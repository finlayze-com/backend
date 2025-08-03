from fastapi import APIRouter, Depends,Request,Query
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



