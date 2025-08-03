import asyncio
from sqlalchemy import select
from backend.db.connection import async_session
from backend.users import models

ROLES = [
    {"name": "superadmin", "description": "دسترسی کامل به همه چیز"},
    {"name": "admin", "description": "مدیریت کاربران و اشتراک‌ها"},
    {"name": "analyst", "description": "تحلیل‌گر فقط برای گزارش‌ها"},
    {"name": "support", "description": "پشتیبانی فقط خواندنی"},
    {"name": "user", "description": "کاربر عادی"},
    {"name": "guest", "description": "مهمان با حداقل دسترسی"},
]

async def seed_roles():
    async with async_session() as db:
        for role in ROLES:
            result = await db.execute(select(models.Role).where(models.Role.name == role["name"]))
            if not result.scalars().first():
                db.add(models.Role(name=role["name"], description=role["description"]))
                print(f"✅ Role created: {role['name']}")
        await db.commit()

if __name__ == "__main__":
    asyncio.run(seed_roles())