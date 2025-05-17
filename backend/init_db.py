# backend/init_db.py

from backend.db.connection import engine
from backend.users.models import Base

print("🛠 در حال ساخت جداول...")
Base.metadata.create_all(bind=engine)
print("✅ همه جداول ساخته شدند.")
