import os
import asyncio
from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from backend.db.connection import async_session
from backend.users import models
from backend.users.routes.auth import get_password_hash
from backend.users.models import UserType

load_dotenv()
ADMIN_PASSWORD = os.environ["ADMIN_INITIAL_PASSWORD"]

ADMIN_DATA = {
    "username": "admin",
    "email": "admin@example.com",
    "password": ADMIN_PASSWORD,
    "first_name": "Admin",
    "last_name": "User",
    "phone_number": "09120000000",
    "user_type": UserType.HAGHIGHI,  # ✅ به‌جای string، مقدار enum
    "is_active": True,
    "is_email_verified": True,
    "role_name": "superadmin"
}

async def seed_admin_user():
    async with async_session() as db:
        # ⬇️ بارگذاری همراه با roles
        result = await db.execute(
            select(models.User).options(selectinload(models.User.roles)).where(models.User.username == ADMIN_DATA["username"])
        )
        user = result.scalars().first()

        if not user:
            hashed_password = get_password_hash(ADMIN_DATA["password"])
            user = models.User(
                username=ADMIN_DATA["username"],
                email=ADMIN_DATA["email"],
                password_hash=hashed_password,
                first_name=ADMIN_DATA["first_name"],
                last_name=ADMIN_DATA["last_name"],
                phone_number=ADMIN_DATA["phone_number"],
                user_type=ADMIN_DATA["user_type"],
                is_active=ADMIN_DATA["is_active"],
                is_email_verified=ADMIN_DATA["is_email_verified"]
            )
            db.add(user)
            await db.commit()
            await db.refresh(user)
            print("✅ Admin user created.")

            # دوباره بارگذاری با roles
            result = await db.execute(
                select(models.User).options(selectinload(models.User.roles)).where(models.User.username == ADMIN_DATA["username"])
            )
            user = result.scalars().first()

        # 🔍 بررسی نقش
        result = await db.execute(select(models.Role).where(models.Role.name == ADMIN_DATA["role_name"]))
        role = result.scalars().first()
        if not role:
            role = models.Role(name=ADMIN_DATA["role_name"], description="دسترسی کامل")
            db.add(role)
            await db.commit()
            await db.refresh(role)
            print("✅ Role 'superadmin' created.")

        if role not in user.roles:
            user.roles.append(role)
            await db.commit()
            print("🔗 Role 'superadmin' assigned to admin user.")

if __name__ == "__main__":
    asyncio.run(seed_admin_user())
