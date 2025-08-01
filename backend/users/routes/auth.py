
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi import APIRouter, Request, Depends, HTTPException, status
from fastapi.security import HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from backend.db.connection import async_session
from backend.users import models
from backend.users.models import User, UserType, Subscription, UserSubscription, Role
from backend.users.schemas import UserCreate, UserLogin
from backend.utils.response import create_response
from fastapi.security import HTTPAuthorizationCredentials
from dotenv import load_dotenv
from pathlib import Path
import os

router = APIRouter()

# 🔐 امنیت رمز عبور و توکن
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")
SECRET_KEY = os.getenv("SECRET_KEY")
print("🧪 Loaded SECRET_KEY:", repr(SECRET_KEY))

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = HTTPBearer(auto_error=True)

# 📦 اتصال دیتابیس
async def get_db():
    async with async_session() as session:
        yield session

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

# ✅ توکن‌سازی همراه با ویژگی‌ها و نقش‌ها
async def create_access_token(user_id: int, db: AsyncSession, expires_delta: Optional[timedelta] = None):
    result = await db.execute(
        select(User)
        .options(
            joinedload(User.roles).joinedload(Role.permissions),
            joinedload(User.subscriptions).joinedload(UserSubscription.subscription)
        )
        .where(User.id == user_id)
    )
    user = result.unique().scalar_one_or_none()
    if not user:
        raise Exception("User not found")

    roles = [role.name for role in user.roles]
    permissions = list({perm.name for role in user.roles for perm in role.permissions})

    features = {}
    now = datetime.utcnow()
    active_sub = next((s for s in user.subscriptions if s.is_active and s.end_date >= now), None)
    if active_sub:
        features = active_sub.subscription.features or {}

    to_encode = {
        "sub": str(user.id),
        "roles": roles,
        "permissions": permissions,
        "features": features,
        "exp": datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    }

    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(
    token: HTTPAuthorizationCredentials = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db)
):
    token = token.credentials
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub"))
        if not user_id:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    result = await db.execute(
        select(models.User)
        .options(
            joinedload(models.User.roles),
            joinedload(models.User.subscriptions).joinedload(models.UserSubscription.subscription)
        )
        .where(models.User.id == user_id)
    )
    user = result.unique().scalar_one_or_none()

    if user is None:
        raise credentials_exception

    user.token_roles = payload.get("roles", [])
    user.permissions = payload.get("permissions", [])
    user.features = payload.get("features", {})

    return user



# ✅ ثبت‌نام کاربر (مسیر عمومی)
@router.post("/register")
async def register(user: UserCreate, db: AsyncSession = Depends(get_db)):
    stmt = select(User).where((User.username == user.username) | (User.email == user.email))
    result = await db.execute(stmt)
    if result.scalar_one_or_none():
        return create_response(
            status="failed",
            message="نام کاربری یا ایمیل تکراری است",
            data={"errors": {"auth": ["این اطلاعات قبلاً ثبت شده است."]}}
        )

    hashed_password = get_password_hash(user.password)
    db_user = User(
        username=user.username,
        email=user.email,
        password_hash=hashed_password,
        first_name=user.first_name,
        last_name=user.last_name,
        phone_number=user.phone_number,
        user_type=UserType(user.user_type),
        national_code=user.national_code,
        company_national_id=user.company_national_id,
        economic_code=user.economic_code,
        is_active=True,
        is_email_verified=False
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)

    user_data = {
        "id": db_user.id,
        "username": db_user.username,
        "email": db_user.email,
        "first_name": db_user.first_name,
        "last_name": db_user.last_name,
        "user_type": str(db_user.user_type)
    }

    return create_response(
        status="success",
        message="ثبت‌نام با موفقیت انجام شد",
        data={"user": user_data}
    )

# ✅ ورود و دریافت توکن (مسیر عمومی)
@router.post("/login")
async def login(user: UserLogin, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.username == user.username))
    db_user = result.scalar_one_or_none()

    if not db_user or not verify_password(user.password, db_user.password_hash):
        return create_response(
            status="failed",
            status_code=422,
            message="نام کاربری یا کلمه عبور اشتباه است",
            data={"errors": {"auth": ["اطلاعات نادرست"]}}
        )

    token = await create_access_token(db_user.id, db)
    return create_response(
        status="success",
        message="ورود موفقیت‌آمیز",
        data={"access_token": token, "token_type": "bearer"}
    )

# ✅ دریافت اطلاعات حساب کاربر جاری (نیازمند middleware)
@router.get("/me")
async def get_me(request: Request, db: AsyncSession = Depends(get_db)):
    print("✅ request.state.user:", getattr(request.state, "user", None))
    user: User = getattr(request.state, "user", None)
    if not user:
        return create_response(status="fail", message="توکن نامعتبر یا کاربر یافت نشد", data={})
    try:
        now = datetime.utcnow()

        result = await db.execute(
            select(UserSubscription)
            .options(joinedload(UserSubscription.subscription))
            .where(
                UserSubscription.user_id == user.id,
                UserSubscription.is_active == True,
                UserSubscription.start_date <= now,
                UserSubscription.end_date >= now
            )
        )
        active_sub = result.scalar_one_or_none()
        active_plan = active_sub.subscription.name if active_sub and active_sub.subscription else None

        user_data = {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "roles": getattr(user, "role_names", []),
            "features": getattr(request.state, "permissions", {}),
            "active_plan": active_plan
        }

        return create_response(
            status="success",
            message="اطلاعات کاربر با موفقیت دریافت شد",
            data={"user": user_data}
        )
    except Exception as e:
        import traceback
        print("🔥 Exception in /me:", e)
        traceback.print_exc()
        raise e
