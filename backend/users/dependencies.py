from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.users.routes.auth import get_current_user
from backend.users import models
from typing import List
from backend.users.routes.auth import get_current_user
from backend.users.models import User

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
async def get_subscription_dependencies(subscription_id: int, db: AsyncSession) -> list[str]:
    query = """
    SELECT
        tc.table_name,
        kcu.column_name
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.constraint_schema = kcu.constraint_schema
    WHERE
        tc.constraint_type = 'FOREIGN KEY'
        AND kcu.referenced_table_name = 'subscriptions'
        AND kcu.referenced_column_name = 'id'
    """

    result = await db.execute(text(query))
    refs = result.fetchall()

    violating_tables = []

    for table_name, column_name in refs:
        check_query = text(f"SELECT 1 FROM {table_name} WHERE {column_name} = :id LIMIT 1")
        check_result = await db.execute(check_query, {"id": subscription_id})
        if check_result.first():
            violating_tables.append(table_name)

    return violating_tables
