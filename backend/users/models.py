from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Enum, Text, JSON
from sqlalchemy.orm import relationship
from datetime import datetime, timedelta
from enum import Enum as PyEnum
from backend.db.connection import Base
from sqlalchemy import Enum as SqlEnum  # اضافه کن بالا


class UserType(PyEnum):
    HAGHIGHI = "haghighi"
    HOGHOGHI = "hoghoghi"

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, index=True)

    # اطلاعات شخصی و تماس
    first_name = Column(String(100))
    last_name = Column(String(100))
    display_name = Column(String(150))
    user_type = Column(
        SqlEnum(UserType, name="user_type_enum"),
        nullable=False,
        default=UserType.HAGHIGHI
    )
    national_code = Column(String(20))
    company_national_id = Column(String(20))
    economic_code = Column(String(20))

    email = Column(String(255), unique=True, nullable=False)
    phone_number = Column(String(20), unique=True)
    landline = Column(String(20))
    address = Column(Text)
    postal_code = Column(String(20))

    username = Column(String(100), unique=True, index=True)
    password_hash = Column(Text, nullable=False)

    is_active = Column(Boolean, default=True)
    is_email_verified = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

    # ارتباط‌ها
    roles = relationship("Role", secondary="user_roles", back_populates="users")
    subscriptions = relationship("UserSubscription", back_populates="user")


class Role(Base):
    __tablename__ = 'roles'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(Text)
    users = relationship("User", secondary="user_roles", back_populates="roles")
    permissions = relationship("Permission", secondary="role_permissions", back_populates="roles")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

class UserRole(Base):
    __tablename__ = 'user_roles'
    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True)
    role_id = Column(Integer, ForeignKey('roles.id'), primary_key=True)
    assigned_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

class Permission(Base):
    __tablename__ = 'permissions'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(Text)
    roles = relationship("Role", secondary="role_permissions", back_populates="permissions")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

class RolePermission(Base):
    __tablename__ = 'role_permissions'
    role_id = Column(Integer, ForeignKey('roles.id'), primary_key=True)
    permission_id = Column(Integer, ForeignKey('permissions.id'), primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

class Subscription(Base):
    __tablename__ = 'subscriptions'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    duration_days = Column(Integer, nullable=False)
    price = Column(Integer, nullable=False)
    features = Column(JSON, default={})
    is_active = Column(Boolean, default=True)
    name_fa = Column(String(100))
    name_en = Column(String(100))
    role_id = Column(Integer, ForeignKey('roles.id'))
    role = relationship("Role")  # optional for easy access
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

class UserSubscription(Base):
    __tablename__ = 'user_subscriptions'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    subscription_id = Column(Integer, ForeignKey('subscriptions.id'))
    start_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    payment_ref = Column(String(100))
    transaction_id = Column(String(100))
    method = Column(String(50), default="manual")
    status = Column(String(20), default="active")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

    user = relationship("User", back_populates="subscriptions")
    subscription = relationship("Subscription")
