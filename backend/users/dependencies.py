from fastapi import Depends, HTTPException, status
from backend.users.routes.auth import get_current_user
from backend.users import models
from typing import List
from backend.users.routes.auth import get_current_user
from backend.users.models import User

def require_roles(*required_roles: str):
    def checker(user: models.User = Depends(get_current_user)):
        user_roles = set(user.token_roles)
        if not user_roles.intersection(required_roles):
            raise HTTPException(status_code=403, detail="🔒 نقش لازم را ندارید")
        return user
    return checker


def require_permissions(*required_permissions: str):
    def checker(user: models.User = Depends(get_current_user)):
        user_perms = set(user.token_permissions)
        if not user_perms.issuperset(required_permissions):
            raise HTTPException(status_code=403, detail="🚫 مجوز دسترسی ندارید")
        return user
    return checker


def require_feature(feature_key: str):
    def checker(user: models.User = Depends(get_current_user)):
        if not user.token_features.get(feature_key, False):
            raise HTTPException(status_code=403, detail=f"⚠️ ویژگی '{feature_key}' در پلن فعال نیست")
        return user
    return checker

#ادمین
def require_roles(allowed_roles: List[str]):
    def role_checker(user: models.User = Depends(get_current_user)):
        if not hasattr(user, "token_roles") or not any(role in allowed_roles for role in user.token_roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access forbidden: insufficient role"
            )
        return user
    return role_checker

def require_permission(permission_name: str):
    def checker(user: models.User = Depends(get_current_user)):
        if not hasattr(user, "permissions") or permission_name not in user.permissions:
            raise HTTPException(
                status_code=403,
                detail=f"⚠️ نیاز به دسترسی '{permission_name}' دارید"
            )
        return user
    return checker



# ✅ بررسی اینکه کاربر پرمیشن خاصی داره یا نه
def require_permission(permission_name: str):
    def dependency(current_user: User = Depends(get_current_user)):
        if permission_name not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"⛔ دسترسی '{permission_name}' مجاز نیست"
            )
        return current_user
    return dependency
