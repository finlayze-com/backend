from sqlalchemy import select
from backend.db.connection import async_session
from backend.users.models import User, Role, Permission

# ✅ واکشی تمام permissionهای یک کاربر از طریق نقش‌ها
async def get_user_permissions(user_id: int):
    async with async_session() as session:
        result = await session.execute(
            select(Permission.name)
            .join(Role.permissions)
            .join(Role.users)
            .where(User.id == user_id)
        )
        return [row[0] for row in result.all()]
