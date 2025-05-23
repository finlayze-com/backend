from datetime import datetime, timedelta
from typing import Optional, List, Dict
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from fastapi.security import HTTPBearer
from backend.users.models import Role
from sqlalchemy.orm import Session
from backend.db.connection import SessionLocal
from backend.users import models
from fastapi.security.http import HTTPAuthorizationCredentials


# Secret Key & Algorithm
SECRET_KEY = "your-very-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

#oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")
oauth2_scheme = HTTPBearer(auto_error=True)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def create_access_token(user: models.User, expires_delta: Optional[timedelta] = None):
    # ⛑ گرفتن نقش‌ها
    try:
        roles = [role.name for role in user.roles]
    except Exception as e:
        print("❗ Warning: user.roles is invalid →", e)
        roles = []

    # ⛑ گرفتن پرمیشن‌ها
    try:
        permissions = list({perm.name for role in user.roles for perm in role.permissions})
    except Exception:
        permissions = []

    # ⛑ گرفتن featureهای اشتراک فعال (اگر وجود داشت)
    features = {}
    try:
        active_sub = next(
            (s for s in user.subscriptions if s.is_active and s.end_date >= datetime.utcnow()), None
        )
        if active_sub:
            features = active_sub.subscription.features or {}
    except Exception as e:
        print("❗ Warning: user.subscriptions is invalid →", e)

    # ساخت payload
    to_encode = {
        "sub": str(user.id),
        "roles": roles,
        "permissions": permissions,
        "features": features,
        "exp": datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    }

    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)



def get_current_user(
    token: HTTPAuthorizationCredentials = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    token = token.credentials  # فقط JWT را جدا کن
    print("💬 RAW JWT:", token)  # ← نمایش توکن خام

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        print("🔓 JWT PAYLOAD:", payload)  # ← نمایش محتوای دیکد شده
        user_id = payload.get("sub")
        print("👤 Extracted user_id:", user_id)
        if user_id is None:
            raise credentials_exception
    except JWTError as e:
        print("❌ JWT Decode Error:", e)
        raise credentials_exception

    user = db.query(models.User).filter(models.User.id == user_id).first()
    if user is None:
        print("❌ No user found for id:", user_id)
        raise credentials_exception

    # داده‌های توکن را ضمیمه‌ی کاربر می‌کنیم
    user.token_roles = payload.get("roles", [])
    user.permissions = payload.get("permissions", [])
    user.features = payload.get("features", {})

    print("✅ User loaded:", user.username)
    return user
