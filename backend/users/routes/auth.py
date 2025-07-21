from datetime import datetime, timedelta
from typing import Optional, List, Dict
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from fastapi.security import HTTPBearer
from backend.users.models import Role
from sqlalchemy.orm import Session
from backend.db.connection import async_session  # جایگزین SessionLocal
from backend.users import models
from fastapi.security.http import HTTPAuthorizationCredentials
from fastapi import APIRouter
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from backend.users.schemas import (
    UserCreate,
    UserLogin,
    UserOut,
    MeResponse,
    UserSubscribeIn
)
from backend.users.models import (
    User,
    UserType,
    Subscription,
    UserSubscription,
    Role
)
from backend.utils.response import create_response

router = APIRouter()


# Secret Key & Algorithm
SECRET_KEY = "your-very-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

#oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")
oauth2_scheme = HTTPBearer(auto_error=True)



async def get_db():
    async with async_session() as session:
        yield session


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


async def create_access_token(user_id: int, db: AsyncSession, expires_delta: Optional[timedelta] = None):
    # بارگذاری کامل کاربر با نقش‌ها و اشتراک‌ها
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

    # ⛑ گرفتن نقش‌ها
    roles = [role.name for role in user.roles]

    # ⛑ گرفتن پرمیشن‌ها
    permissions = list({perm.name for role in user.roles for perm in role.permissions})

    # ⛑ گرفتن featureهای اشتراک فعال
    features = {}
    try:
        now = datetime.utcnow()
        active_sub = next(
            (s for s in user.subscriptions if s.is_active and s.end_date >= now),
            None
        )
        if active_sub:
            features = active_sub.subscription.features or {}
    except Exception as e:
        print("❗ Warning while loading subscription features:", e)

    # ساخت payload توکن
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



# ✅ ثبت‌نام کاربر
@router.post("/register")
async def register(user: UserCreate, db: AsyncSession = Depends(get_db)):
    stmt = select(User).where(
        (User.username == user.username) | (User.email == user.email)
    )
    result = await db.execute(stmt)
    existing_user = result.scalar_one_or_none()
    if existing_user:
        return create_response(
            status="failed",
            message="خطای اعتبارسنجی اطلاعات ارسال شده",
            data={"errors": {"auth": ["نام کاربری یا ایمیل قبلاً ثبت شده است."]}}
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

    # خروجی اصلی (می‌تونی فیلدهای اضافی رو حذف یا محدود کنی)
    user_data = {
        "id": db_user.id,
        "username": db_user.username,
        "email": db_user.email,
        "first_name": db_user.first_name,
        "last_name": db_user.last_name,
        "phone_number": db_user.phone_number,
        "user_type": str(db_user.user_type),
    }

    return create_response(
        status="success",
        message="ثبت‌نام با موفقیت انجام شد",
        data={"user": user_data}
    )


# ✅ ورود کاربر و دریافت توکن
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

# # ✅ دریافت اطلاعات حساب جاری
# @router.get("/me")
# def get_me(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
#     now = datetime.utcnow()
#
#     active_sub = db.query(models.UserSubscription).filter(
#         models.UserSubscription.user_id == current_user.id,
#         models.UserSubscription.is_active == True,
#         models.UserSubscription.start_date <= now,
#         models.UserSubscription.end_date >= now
#     ).first()
#
#     active_plan = active_sub.subscription.name if active_sub and active_sub.subscription else None
#
#     user_data = {
#         "id": current_user.id,
#         "username": current_user.username,
#         "email": current_user.email,
#         "first_name": current_user.first_name,
#         "last_name": current_user.last_name,
#         "roles": current_user.token_roles,
#         "features": current_user.features or {},
#         "active_plan": active_plan
#     }
#
#     return create_response(
#         status="success",
#         message="اطلاعات کاربر با موفقیت دریافت شد",
#         data={"user": user_data}
#     )

# ✅ ثبت اشتراک کاربر
# @router.post("/subscribe")
# def subscribe_to_plan(
#     data: UserSubscribeIn,
#     db: Session = Depends(get_db),
#     current_user: User = Depends(get_current_user)
# ):
#     subscription = db.query(Subscription).filter(
#         Subscription.id == data.subscription_id,
#         Subscription.is_active == True
#     ).first()
#
#     if not subscription:
#         return create_response(
#             status="failed",
#             message="پلن یافت نشد یا غیرفعال است.",
#             data={"errors": {"subscription": ["پلن یافت نشد یا غیرفعال است."]}}
#         )
#
#     active_exists = db.query(UserSubscription).filter(
#         UserSubscription.user_id == current_user.id,
#         UserSubscription.subscription_id == subscription.id,
#         UserSubscription.is_active == True
#     ).first()
#
#     if active_exists:
#         return create_response(
#             status="failed",
#             message="این اشتراک قبلاً فعال شده است.",
#             data={"errors": {"subscription": ["این اشتراک قبلاً فعال شده است."]}}
#         )
#
#     now = datetime.utcnow()
#     end_date = now + timedelta(days=subscription.duration_days)
#
#     new_sub = UserSubscription(
#         user_id=current_user.id,
#         subscription_id=subscription.id,
#         start_date=now,
#         end_date=end_date,
#         is_active=True,
#         method=data.method,
#         status="active"
#     )
#     db.add(new_sub)
#
#     if subscription.role_id:
#         role = db.query(Role).filter_by(id=subscription.role_id).first()
#         if role and role not in current_user.roles:
#             current_user.roles.append(role)
#
#     db.commit()
#     db.refresh(new_sub)
#
#     return create_response(
#         status="success",
#         message="✅ اشتراک با موفقیت ثبت شد",
#         data={"subscription_id": new_sub.id}
#     )

# # ✅ دریافت اطلاعات حساب جاری
@router.get("/me")
async def get_me(
    current_user: User = Depends(get_current_user),  # ✅ درست شد
    db: AsyncSession = Depends(get_db)
):
    now = datetime.utcnow()

    result = await db.execute(
        select(models.UserSubscription)
        .options(joinedload(models.UserSubscription.subscription))
        .where(
            models.UserSubscription.user_id == current_user.id,
            models.UserSubscription.is_active == True,
            models.UserSubscription.start_date <= now,
            models.UserSubscription.end_date >= now
        )
    )
    active_sub = result.scalar_one_or_none()
    active_plan = active_sub.subscription.name if active_sub and active_sub.subscription else None

    user_data = {
        "id": current_user.id,
        "username": current_user.username,
        "email": current_user.email,
        "first_name": current_user.first_name,
        "last_name": current_user.last_name,
        "roles": current_user.token_roles,
        "features": current_user.features or {},
        "active_plan": active_plan
    }

    return create_response(
        status="success",
        message="اطلاعات کاربر با موفقیت دریافت شد",
        data={"user": user_data}
    )
