from fastapi import Depends, HTTPException, status
from backend.users.auth import get_current_user
from backend.users import models


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
