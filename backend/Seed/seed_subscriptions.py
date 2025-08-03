import asyncio
from sqlalchemy import select
from backend.db.connection import async_session
from backend.users import models

SUBSCRIPTIONS = [
    {"name": "Free", "name_fa": "رایگان", "name_en": "Free", "duration_days": 7, "price": 0, "features": {"max_reports": 3}, "role_name": "guest"},
    {"name": "Basic", "name_fa": "پایه", "name_en": "Basic", "duration_days": 30, "price": 100000, "features": {"max_reports": 10}, "role_name": "user"},
    {"name": "Pro", "name_fa": "پیشرفته", "name_en": "Pro", "duration_days": 90, "price": 250000, "features": {"max_reports": 50, "export": True}, "role_name": "analyst"},
    {"name": "Enterprise", "name_fa": "سازمانی", "name_en": "Enterprise", "duration_days": 365, "price": 1000000, "features": {"all_features": True}, "role_name": "admin"}
]

async def seed_subscriptions():
    async with async_session() as db:
        for plan in SUBSCRIPTIONS:
            result = await db.execute(select(models.Subscription).where(models.Subscription.name == plan["name"]))
            existing = result.scalars().first()

            role_result = await db.execute(select(models.Role).where(models.Role.name == plan["role_name"]))
            role = role_result.scalars().first()

            if not role:
                print(f"⚠️ Role '{plan['role_name']}' not found for subscription '{plan['name']}' — skipping.")
                continue

            if not existing:
                db.add(models.Subscription(
                    name=plan["name"],
                    name_fa=plan["name_fa"],
                    name_en=plan["name_en"],
                    duration_days=plan["duration_days"],
                    price=plan["price"],
                    features=plan["features"],
                    role_id=role.id,
                    is_active=True
                ))
                print(f"✅ Subscription created: {plan['name']}")
        await db.commit()

if __name__ == "__main__":
    asyncio.run(seed_subscriptions())
