from pydantic import BaseModel, EmailStr, constr
from typing import Optional, List, Dict,Any
from enum import Enum
from datetime import datetime, timedelta,timezone
from pydantic import BaseModel, EmailStr, constr, field_validator, model_validator
import re


# ----------------------------

# --- الگوها و کاراکترهای مجاز/ممنوع ---
USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{6,}$")  # شروع با حرف + حداقل 7 کاراکتر
NAME_RE = re.compile(r"^[\u0600-\u06FFa-zA-Z\s'-]+$")      # فارسی/انگلیسی + فاصله/'/-
IR_MOBILE_RE = re.compile(r"^(?:\+?98|0)?9\d{9}$")          # 09xxxxxxxxx یا +989xxxxxxxxx
ONLY_DIGITS_RE = re.compile(r"^\d+$")
FORBIDDEN_IN_USERNAME = set("@#%^&*()!×÷`")

PASSWORD_MIN_LEN = 5
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
    password_confirm: str
    phone_number: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    user_type: Optional[UserType] = UserType.haghighi
    national_code: Optional[str] = None
    company_national_id: Optional[str] = None
    economic_code: Optional[str] = None

    # --- نرمال‌سازی اولیه فیلدهای متنی ---
    @field_validator("username", "first_name", "last_name", "phone_number",
                     "national_code", "company_national_id", "economic_code",
                     mode="before")
    @classmethod
    def strip_strings(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v

        # --- username rules ---
    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str):
            # عدم وجود کاراکترهای ممنوع
            if any(ch in FORBIDDEN_IN_USERNAME for ch in v):
                raise ValueError("نام کاربری شامل کاراکترهای ممنوع است")
            # الگوی مجاز: شروع با حرف، حداقل 7 کاراکتر، فقط حروف/اعداد/._-
            if not USERNAME_RE.match(v):
                raise ValueError(
                    "نام کاربری باید با حرف انگلیسی شروع شود و حداقل ۷ کاراکتر باشد و فقط شامل حروف، اعداد و . _ - باشد")
            return v
            # --- first_name / last_name rules ---

    @field_validator("first_name", "last_name")
    @classmethod
    def validate_names(cls, v: Optional[str], info):
        if v is None:
            return v
        if not NAME_RE.match(v):
            raise ValueError(f"{info.field_name} فقط شامل حروف (فارسی/انگلیسی)، فاصله، - و ' باشد")
        return v

        # --- phone_number: ایران ---
    @field_validator("phone_number")
    @classmethod
    def validate_phone(cls, v: str):
            if not IR_MOBILE_RE.match(v):
                raise ValueError("شماره موبایل معتبر نیست (الگوی مجاز: 09xxxxxxxxx یا +989xxxxxxxxx)")
            return v

            # --- national_code: 10 رقمی + چک‌سام ---

    @field_validator("national_code")
    @classmethod
    def validate_national_code(cls, v: Optional[str]):
        if v is None or v == "":
            return v
        if not (len(v) == 10 and ONLY_DIGITS_RE.match(v)):
            raise ValueError("کد ملی باید ۱۰ رقم باشد")
        # چک‌سام کد ملی ایران
        digits = list(map(int, v))
        if len(set(digits)) == 1:  # همه ارقام یکسان
            raise ValueError("کد ملی نامعتبر است")
        checksum = digits[-1]
        s = sum(d * (10 - i) for i, d in enumerate(digits[:9]))
        r = s % 11
        valid = (r < 2 and checksum == r) or (r >= 2 and checksum == (11 - r))
        if not valid:
            raise ValueError("کد ملی نامعتبر است")
        return v

        # --- company_national_id: اگر حقوقی → اجباری و 11 رقمی ---
    @field_validator("company_national_id")
    @classmethod
    def validate_company_id(cls, v: Optional[str]):
            if v is None or v == "":
                return v
            if not (len(v) == 11 and ONLY_DIGITS_RE.match(v)):
                raise ValueError("شناسه ملی شرکت باید ۱۱ رقم باشد")
            return v

    # --- economic_code: 11 تا 16 رقم ---
    @field_validator("economic_code")
    @classmethod
    def validate_economic_code(cls, v: Optional[str]):
        if v is None or v == "":
            return v
        if not (11 <= len(v) <= 16 and ONLY_DIGITS_RE.match(v)):
            raise ValueError("کد اقتصادی باید عددی و بین ۱۱ تا ۱۶ رقم باشد")
        return v

        # --- password rules: حداقل 5 کاراکتر + حداقل یک عدد و یک حرف ---
    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str):
            if len(v) < PASSWORD_MIN_LEN:
                raise ValueError(f"رمز عبور باید حداقل {PASSWORD_MIN_LEN} کاراکتر باشد")
            if not re.search(r"[A-Za-z]", v) or not re.search(r"\d", v):
                raise ValueError("رمز عبور باید ترکیبی از حروف و اعداد باشد")
            return v

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
    # ✅ بررسی تساوی password و password_confirm
    @field_validator("password_confirm")
    @classmethod
    def passwords_match(cls, v, values):
        password = values.get("password")
        if password and v != password:
            raise ValueError("Passwords do not match")
        return v

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

