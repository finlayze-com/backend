import asyncio
from backend.Seed.seed_roles import seed_roles
from backend.Seed.seed_permissions import seed_permissions_and_roles
from backend.Seed.seed_subscriptions import seed_subscriptions
from backend.Seed.seed_admin_user import seed_admin_user

async def run_all_seeds():
    print("🔧 Seeding roles...")
    await seed_roles()

    print("🔧 Seeding permissions + role mappings...")
    await seed_permissions_and_roles()

    print("🔧 Seeding subscription plans...")
    await seed_subscriptions()

    print("🔧 Seeding admin user...")
    await seed_admin_user()

if __name__ == "__main__":
    asyncio.run(run_all_seeds())