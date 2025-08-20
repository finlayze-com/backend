from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.users.routes.auth import get_current_user
from backend.users import models
from typing import List
from backend.users.routes.auth import get_current_user
from backend.users.models import User
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def require_roles(*allowed_roles: str):
    async def role_checker(user: User = Depends(get_current_user)):
        if not any(role.name in allowed_roles for role in user.roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied: insufficient role"
            )
        return user
    return role_checker

def require_permissions(*required_permissions: str):
    async def permission_checker(user: User = Depends(get_current_user)):
        print("✅ user.permissions =", user.permissions)
        print("✅ required_permissions =", required_permissions)
        if not any(p in user.permissions for p in required_permissions):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied: insufficient permission"
            )
        return user
    return permission_checker

def require_feature(feature_name: str):
    async def feature_checker(user: User = Depends(get_current_user)):
        if not user.features or feature_name not in user.features:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied: feature '{feature_name}' not available"
            )
        return user
    return feature_checker

# ✅ بررسی وجود وابستگی‌های ForeignKey به subscription_id در سایر جداول دیتابیس
# ✅ بررسی وجود وابستگی‌های ForeignKey به subscription_id در سایر جداول دیتابیس
# ✅ بررسی وجود وابستگی‌های FK به subscriptions(id)
async def get_subscription_dependencies(subscription_id: int, db: AsyncSession) -> list[str]:
    # این Query تمام FKهایی که به subscriptions(id) اشاره می‌کنند را برمی‌گرداند
    query = """
    SELECT
        tc.table_name,         -- جدولی که FK روی آن تعریف شده (جدول فرزند)
        kcu.column_name        -- ستونی که FK است در جدول فرزند
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.constraint_schema = kcu.constraint_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
     AND ccu.constraint_schema = tc.constraint_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND ccu.table_name = 'subscriptions'
      AND ccu.column_name = 'id'
      AND ccu.table_schema = current_schema()   -- اگر از اسکیمای پیش‌فرض استفاده می‌کنی، همین خوبه
    """

    result = await db.execute(text(query))
    refs = result.fetchall()  # [(table_name, column_name), ...]

    violating_tables = []
    for table_name, column_name in refs:
        check_query = text(f"SELECT 1 FROM {table_name} WHERE {column_name} = :sid LIMIT 1")
        check_result = await db.execute(check_query, {"sid": subscription_id})
        if check_result.first():
            violating_tables.append(table_name)

    return violating_tables
