
import asyncio
from sqlalchemy import select, func
from backend.db.connection import async_session
from backend.users import models

async def print_count(model, label):
    async with async_session() as db:
        result = await db.execute(select(func.count()).select_from(model))
        count = result.scalar_one()
        print(f"{label:<20}: {count}")

async def seed_report():
    print("\n📊 Seed Report Summary:")
    await print_count(models.Role, "Roles")
    await print_count(models.Permission, "Permissions")
    await print_count(models.User, "Users")
    await print_count(models.Subscription, "Subscriptions")
    await print_count(models.UserSubscription, "UserSubscriptions")
    await print_count(models.UserRole, "UserRoles")
    await print_count(models.RolePermission, "RolePermissions")
    print("✅ Done.\n")

if __name__ == "__main__":
    asyncio.run(seed_report())
