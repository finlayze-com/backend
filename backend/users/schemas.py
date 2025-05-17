from pydantic import BaseModel, EmailStr, constr
from typing import Optional, List, Dict
from enum import Enum
from datetime import datetime


class UserType(str, Enum):
    haghighi = "haghighi"
    hoghoghi = "hoghoghi"


# ----------------------------
# 📥 ورودی‌ها (Inputs)
# ----------------------------

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


class UserLogin(BaseModel):
    username: str
    password: str


# ----------------------------
# 📤 خروجی‌ها (Outputs)
# ----------------------------

class RoleOut(BaseModel):
    id: int
    name: str
    class Config:
        orm_mode = True


class PermissionOut(BaseModel):
    id: int
    name: str
    class Config:
        orm_mode = True


class SubscriptionOut(BaseModel):
    id: int
    name: str
    duration_days: int
    price: int
    features: Dict[str, bool]
    is_active: bool
    class Config:
        orm_mode = True


class UserOut(BaseModel):
    id: int
    username: str
    email: EmailStr
    display_name: Optional[str]
    user_type: UserType
    roles: List[RoleOut] = []
    class Config:
        orm_mode = True
