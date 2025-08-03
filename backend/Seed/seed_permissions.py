import asyncio
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from backend.db.connection import async_session
from backend.users import models

PERMISSIONS_TO_CREATE = [
    "Subscription.ViewAll", "Subscription.Create", "Subscription.Update", "Subscription.Delete", "Subscription.ViewById",
    "User.ViewAll",
    "UserSubscription.ViewAll", "UserSubscription.Create", "UserSubscription.Update", "UserSubscription.Delete",
    "Role.Create", "Role.ViewAll", "Role.AssignToUser", "Role.RemoveFromUser",
    "Permission.Create", "Permission.ViewAll", "Permission.AssignToRole",
    "Report.CandlestickRaw", "Report.Metadata.Sectors", "Report.Metadata.Stocks", "Report.Metadata.SectorMap",
    "Report.Treemap", "Report.Sankey", "Report.RealMoneyFlow", "Report.OrderBook.BumpChart", "Report.OrderBook.TimeSeries",
    "ALL"
]

ROLES_TO_CREATE = {
    "superadmin": PERMISSIONS_TO_CREATE,
    "admin": ["Subscription.ViewAll", "User.ViewAll", "UserSubscription.ViewAll", "Report.*"],
    "viewer": ["Report.CandlestickRaw", "Report.Treemap", "Report.RealMoneyFlow"]
}

async def seed_permissions_and_roles():
    async with async_session() as db:
        # Step 1: Add permissions
        for perm_name in PERMISSIONS_TO_CREATE:
            result = await db.execute(select(models.Permission).where(models.Permission.name == perm_name))
            if not result.scalars().first():
                db.add(models.Permission(name=perm_name))
                print(f"✅ Permission created: {perm_name}")
        await db.commit()

        # Step 2: Fetch all permissions
        result = await db.execute(select(models.Permission))
        all_permissions = result.scalars().all()

        for role_name, perm_list in ROLES_TO_CREATE.items():
            # Load role + permissions with selectinload
            result = await db.execute(
                select(models.Role).options(selectinload(models.Role.permissions)).where(models.Role.name == role_name)
            )
            role = result.scalars().first()

            # اگر نقش وجود ندارد، بساز و دوباره بارگذاری کن با selectinload
            if not role:
                new_role = models.Role(name=role_name, description=f"Role: {role_name}")
                db.add(new_role)
                await db.commit()

                # 🔁 Load again to attach permissions safely
                result = await db.execute(
                    select(models.Role).options(selectinload(models.Role.permissions)).where(models.Role.name == role_name)
                )
                role = result.scalars().first()

            # Assign permissions
            for perm in all_permissions:
                match_explicit = perm.name in perm_list
                match_wildcard = any(perm.name.startswith(p.replace("*", "")) for p in perm_list if "*" in p)
                if (match_explicit or match_wildcard) and perm not in role.permissions:
                    role.permissions.append(perm)

            await db.commit()
            print(f"🔗 Permissions assigned to role {role_name}")

if __name__ == "__main__":
    asyncio.run(seed_permissions_and_roles())
