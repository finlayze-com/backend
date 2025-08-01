from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request
from starlette.responses import JSONResponse
from jose import jwt, JWTError, ExpiredSignatureError
from uuid import uuid4
from time import time
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from fastapi.security import HTTPAuthorizationCredentials
from backend.db.connection import async_session
from backend.users.models import User
from backend.users.utils import get_user_permissions
from backend.utils.logger import logger
from pathlib import Path
import os
from dotenv import load_dotenv

# 🔐 امنیت رمز عبور و توکن
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")
SECRET_KEY = os.getenv("SECRET_KEY")
assert SECRET_KEY == "Afiroozi12!@^erySecretKey9876*", "❌ SECRET_KEY mismatch!"
print("🧪 Loaded SECRET_KEY from middleware:", repr(os.getenv("SECRET_KEY")))
ALGORITHM = "HS256"

# مسیرهایی که نیاز به احراز هویت ندارند
PUBLIC_PATHS = [
    "/docs", "/docs/", "/openapi.json", "/favicon.ico", "/ping", "/static",
    "/login", "/register","/seed/superadmin"
]

def is_public_path(path: str) -> bool:
    return (
        path in PUBLIC_PATHS or
        path.startswith("/static") or
        path.startswith("/docs") or
        path.startswith("/redoc") or
        path.startswith("/openapi.json")
    )

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        print("📍 وارد middleware شدیم:", request.url.path)
        path = request.url.path
        request_id = str(uuid4())
        request.state.request_id = request_id

        logger.info(f"[{request_id}] درخواست جدید: {request.method} {path}")

        # 🟡 اضافه کن این خط برای دیباگ:
        print("📥 All headers:", dict(request.headers))

        start_time = time()

        if is_public_path(path):
            logger.info(f"[{request_id}] مسیر عمومی → بدون نیاز به توکن")
            response = await call_next(request)
            duration = time() - start_time
            logger.info(f"[{request_id}] پاسخ با status {response.status_code} در {duration:.3f} ثانیه")
            return response

        auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            logger.warning(f"[{request_id}] هدر Authorization ارسال نشده یا ناقص است")
            return JSONResponse(status_code=401, content={"detail": "توکن ارسال نشده"})

        token = auth_header.split(" ")[1]
        print("🧪 توکن دریافتی از هدر:", repr(token))

        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            print("🧪 SECRET_KEY در هنگام decode:", repr(SECRET_KEY))
            user_id = int(payload.get("sub"))
        except ExpiredSignatureError:
            logger.warning(f"[{request_id}] توکن منقضی شده")
            return JSONResponse(status_code=401, content={"detail": "توکن منقضی شده"})
        except JWTError as e:
            logger.warning(f"[{request_id}] خطا در دیکد کردن توکن: {e}")
            return JSONResponse(status_code=401, content={"detail": "توکن نامعتبر است"})

        async with async_session() as session:
            result = await session.execute(
                select(User).options(joinedload(User.roles)).where(User.id == user_id)
            )
            user = result.scalars().first()

        if not user:
            logger.warning(f"[{request_id}] کاربر یافت نشد")
            return JSONResponse(status_code=401, content={"detail": "کاربر یافت نشد"})

        if not user.is_active:
            logger.warning(f"[{request_id}] حساب کاربری غیرفعال است")
            return JSONResponse(status_code=403, content={"detail": "حساب کاربری شما غیرفعال است"})

        if user.deleted_at is not None:
            logger.warning(f"[{request_id}] حساب کاربری حذف‌شده است")
            return JSONResponse(status_code=403, content={"detail": "حساب کاربری شما حذف شده است"})

        # دریافت دسترسی‌ها و نقش‌ها
        permissions = await get_user_permissions(user_id)
        user.role_names = [r.name for r in user.roles] if user.roles else []

        # برای دیباگ در ترمینال چاپ کن
        print(" توکن:", token)
        print(" Payload:", payload)
        print(" کاربر:", user)
        print(" نقش‌ها:", user.role_names)

        # ذخیره در request.state
        request.state.user = user
        request.state.permissions = permissions
        request.state.role_names = user.role_names

        logger.info(f"[{request_id}] کاربر {user.id} → نقش‌ها: {user.role_names}")

        try:
            response = await call_next(request)
        except Exception as e:
            logger.exception(f"[{request_id}] خطای داخلی سرور: {e}")
            raise e

        duration = time() - start_time
        logger.info(f"[{request_id}] پاسخ نهایی با status {response.status_code} در {duration:.3f} ثانیه")
        return response
