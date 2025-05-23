from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from backend.db.connection import SessionLocal
from backend.users import models, schemas
from backend.users.auth import get_current_user, create_access_token
from passlib.context import CryptContext
from datetime import datetime, timedelta
from backend.users.models import UserType
from typing import List
from backend.users.dependencies import require_roles


router = APIRouter()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ✅ ثبت اشتراک
@router.post("/subscribe")
def subscribe_to_plan(
    data: schemas.UserSubscribeIn,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user)
):
    subscription = db.query(models.Subscription).filter(
        models.Subscription.id == data.subscription_id,
        models.Subscription.is_active == True
    ).first()

    if not subscription:
        raise HTTPException(status_code=404, detail="پلن یافت نشد یا غیرفعال است")

    # چک نکنه همین پلن فعال رو قبلاً داره
    active_exists = db.query(models.UserSubscription).filter(
        models.UserSubscription.user_id == user.id,
        models.UserSubscription.subscription_id == subscription.id,
        models.UserSubscription.is_active == True
    ).first()

    if active_exists:
        raise HTTPException(status_code=400, detail="این اشتراک قبلاً برای کاربر فعال شده است")

    # محاسبه تاریخ‌ها
    now = datetime.utcnow()
    end_date = now + timedelta(days=subscription.duration_days)

    # ساخت اشتراک
    new_sub = models.UserSubscription(
        user_id=user.id,
        subscription_id=subscription.id,
        start_date=now,
        end_date=end_date,
        is_active=True,
        method=data.method,
        status="active"
    )
    db.add(new_sub)

    # اختصاص نقش پلن به کاربر
    if subscription.role_id:
        role = db.query(models.Role).filter(models.Role.id == subscription.role_id).first()
        if role and role not in user.roles:
            user.roles = [role]

    db.commit()
    db.refresh(new_sub)

    return {"message": "✅ اشتراک با موفقیت ثبت شد", "subscription_id": new_sub.id}


# ✅ رمزنگاری
def get_password_hash(password):
    return pwd_context.hash(password)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


# ✅ ثبت‌نام
@router.post("/register", response_model=schemas.UserOut)
def register(user: schemas.UserCreate, db: Session = Depends(get_db)):
    existing_user = db.query(models.User).filter(
        (models.User.username == user.username) |
        (models.User.email == user.email)
    ).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Username or email already registered")

    hashed_password = get_password_hash(user.password)
    db_user = models.User(
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
        is_email_verified=False,
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


# ✅ ورود و ساخت توکن
@router.post("/login")
def login(user: schemas.UserLogin, db: Session = Depends(get_db)):
    db_user = db.query(models.User).filter(models.User.username == user.username).first()
    if not db_user or not verify_password(user.password, db_user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    access_token = create_access_token(db_user)
    return {"access_token": access_token, "token_type": "bearer"}

#نمایش پلن‌ها رو بسازیم تا فرانت بتونه لیست همه پلن‌های فعال رو بگیره
@router.get("/subscriptions", response_model=List[schemas.SubscriptionOut])
def list_active_subscriptions(db: Session = Depends(get_db)):
    subs = db.query(models.Subscription).filter(models.Subscription.is_active == True).all()
    return subs


@router.get("/me", response_model=schemas.MeResponse)
def get_me(user: models.User = Depends(get_current_user)):
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "roles": user.token_roles,       # ✅
        "features": user.features or {}  # ✅
    }

@router.get("/my-subscriptions", response_model=List[schemas.UserSubscriptionOut])
def get_my_subscriptions(
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user)
):
    return db.query(models.UserSubscription)\
        .filter(models.UserSubscription.user_id == user.id)\
        .order_by(models.UserSubscription.start_date.desc())\
        .all()


#ادمین
@router.get("/admin/users")
def list_users_for_admin(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_roles(["admin", "superadmin"]))
):
    users = db.query(models.User).all()
    return [
        {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "is_active": user.is_active,
            "roles": [role.name for role in user.roles]
        }
        for user in users
    ]

@router.get("/admin/subscriptions", response_model=List[schemas.SubscriptionOut])
def get_all_subscriptions(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_roles(["superadmin"]))
):
    return db.query(models.Subscription).order_by(models.Subscription.id).all()
