from pydantic import BaseModel, EmailStr, constr
from typing import Optional, List, Dict,Any
from enum import Enum
from datetime import datetime, timedelta,timezone
from pydantic import field_validator

# ----------------------------
# 🎭 نوع کاربر (حقیقی / حقوقی)
# ----------------------------

class UserType(str, Enum):
    haghighi = "haghighi"
    hoghoghi = "hoghoghi"


# ----------------------------
# 📥 ورودی‌ها (Inputs)
# ----------------------------
## 🧾 ورودی ثبت‌نام کاربر

class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: constr(min_length=6)
    phone_number: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    user_type: Optional[UserType] = UserType.haghighi
    national_code: Optional[str] = None
    company_national_id: Optional[str] = None
    economic_code: Optional[str] = None

    @field_validator("user_type", mode="before")
    @classmethod
    def to_enum_user_type_create(cls, v):
        if v is None:
            return v
        s = str(getattr(v, "value", v)).strip().upper()
        if s == "HAGHIGHI":
            return UserType.haghighi
        if s == "HOGHOGHI":
            return UserType.hoghoghi
        raise ValueError("user_type must be 'haghighi' or 'hoghoghi'")


## 🧾 ورودی ورود به حساب
class UserLogin(BaseModel):
    username: str
    password: str

# 🧾 ورودی بروزرسانی کاربر
class UserUpdate(BaseModel):
    username: Optional[str] = None
    email: Optional[EmailStr] = None
    password: Optional[constr(min_length=6)] = None
    phone_number: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    user_type: Optional[UserType] = None
    national_code: Optional[str] = None
    company_national_id: Optional[str] = None
    economic_code: Optional[str] = None
    is_active: Optional[bool] = None

    @field_validator("user_type", mode="before")
    @classmethod
    def to_enum_user_type_create(cls, v):
        if v is None:
            return v
        s = str(getattr(v, "value", v)).strip().upper()
        if s == "HAGHIGHI":
            return UserType.haghighi
        if s == "HOGHOGHI":
            return UserType.hoghoghi
        raise ValueError("user_type must be 'haghighi' or 'hoghoghi'")


## 🧾 ورودی خرید اشتراک
class UserSubscribeIn(BaseModel):
    subscription_id: int
    method: str = "manual"


## 🧾 ساخت پلن جدید توسط سوپرادمین
class SubscriptionCreate(BaseModel):
    name: str
    name_fa: Optional[str]
    name_en: Optional[str]
    duration_days: int
    price: int
    features: Dict[str, Any]
    role_id: Optional[int]  # نقشی که با این پلن مرتبط است

    class Config:
        from_attributes = True

## 🧾 بروزرسانی پلن (توسط سوپرادمین)
class SubscriptionUpdate(BaseModel):
    name: Optional[str] = None
    name_fa: Optional[str] = None
    name_en: Optional[str] = None
    duration_days: Optional[int] = None
    price: Optional[int] = None
    features: Optional[Dict[str, Any]] = None
    role_id: Optional[int] = None
    is_active: Optional[bool] = None

## 🧾 ساخت نقش جدید
class RoleCreate(BaseModel):
    name: str
    description: Optional[str] = None

## 🧾 اختصاص نقش به کاربر
class AssignRoleInput(BaseModel):
    role_id: int

## 🧾 حذف نقش از کاربر
class RemoveRoleInput(BaseModel):
    role_id: int

## 🧾 ساخت permission جدید
class PermissionCreate(BaseModel):
    name: str
    description: Optional[str] = None

## 🧾 اختصاص permission به نقش
class AssignPermissionInput(BaseModel):
    permission_ids: list[int]

# ✅ ورودی برای ساخت اشتراک جدید توسط ادمین
class UserSubscriptionCreateAdmin(BaseModel):
    user_id: int
    subscription_id: int
    start_date: datetime
    end_date: Optional[datetime] = None   # ← اختیاری شد
    is_active: bool = True
    method: str = "manual"
    status: str = "active"

    # 👇 این ولیدیتور رشتهٔ خالی را None می‌کند
    @field_validator("end_date", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "" or v is None:
            return None
        return v

        # همه تاریخ‌ها => UTC naive (برای ستون‌های WITHOUT TIME ZONE)
    @field_validator("start_date", "end_date", mode="after")
    @classmethod
    def to_naive_utc(cls, v: Optional[datetime]):
            if v is None:
                return v
            if v.tzinfo is not None:
                v = v.astimezone(timezone.utc).replace(tzinfo=None)
            return v


# ✅ ورودی برای ویرایش اشتراک توسط ادمین
class UserSubscriptionUpdateAdmin(BaseModel):
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    is_active: Optional[bool] = None
    method: Optional[str] = None
    status: Optional[str] = None

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def parse_to_naive_utc(cls, v):
        if v in (None, "", "null"):
            return None
        # اگر استرینگ ISO با Z بود
        if isinstance(v, str):
            v = v.replace("Z", "+00:00")
            v = datetime.fromisoformat(v)
        # اگر datetime بود، tz رو به UTC و بعد tzinfo رو حذف کن
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                v = v.astimezone(timezone.utc).replace(tzinfo=None)
            return v
        raise ValueError("Invalid datetime")

# ----------------------------
# 📤 خروجی‌ها (Outputs)
# ----------------------------

# ✅ نمایش نقش ها
class RoleOut(BaseModel):
    id: int
    name: str
    description: Optional[str]

    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime] = None
    class Config:
        orm_mode = True

# ✅ نمایش permission
class PermissionOut(BaseModel):
    id: int
    name: str
    class Config:
        orm_mode = True

# ✅ نمایش نقش به همراه لیست permissionها
class RoleWithPermissions(BaseModel):
    id: int
    name: str
    description: Optional[str]
    permissions: List[PermissionOut]

    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime] = None

    class Config:
        from_attributes = True

# ✅ نمایش اطلاعات پلن اشتراک
class SubscriptionOut(BaseModel):
    id: int
    name: str
    name_fa: Optional[str]
    name_en: Optional[str]
    duration_days: int
    price: int
    features: Dict[str, Any]
    is_active: bool
    role_id: Optional[int]

    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime] = None
    class Config:
        orm_mode = True

# ✅ نمایش اطلاعات کاربر
class UserOut(BaseModel):
    id: int
    username: str
    email: EmailStr
    display_name: Optional[str]
    user_type: UserType
    roles: List[RoleOut] = []

    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime] = None

    class Config:
        orm_mode = True


# ✅ اطلاعات کاربر جاری (برای /me)
class MeResponse(BaseModel):
    id: int
    username: str
    email: str
    first_name: Optional[str]
    last_name: Optional[str]
    roles: List[str]
    features: Dict[str, Any]
    active_plan: Optional[str]  # ← اضافه شده

    class Config:
        from_attributes = True



## ✅ نمایش اطلاعات خلاصه یک پلن
class SimpleSubscription(BaseModel):
    name: str
    name_en: Optional[str]
    features: Dict[str, Any]

    class Config:
        from_attributes = True

# ✅ نمایش اطلاعات خرید اشتراک توسط کاربر
class UserSubscriptionOut(BaseModel):
    id: int
    subscription_id: int
    start_date: datetime
    end_date: datetime
    is_active: bool
    method: str
    status: str
    subscription: Optional[SimpleSubscription]

    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime] = None

    class Config:
        from_attributes = True

