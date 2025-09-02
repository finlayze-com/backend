from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import IntegrityError
from jose.exceptions import JWTError, ExpiredSignatureError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.status import (
    HTTP_400_BAD_REQUEST, HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT, HTTP_422_UNPROCESSABLE_ENTITY, HTTP_500_INTERNAL_SERVER_ERROR,
)

from backend.utils.response import create_response
from backend.utils.exceptions import AppException  # ← اگر قدم 0 را اضافه کردی

# --- Helper: تشخیص unique violation برای 409
def _is_unique_violation(exc: IntegrityError) -> bool:
    try:
        if getattr(exc.orig, "pgcode", None) == "23505":  # postgres unique_violation
            return True
        msg = str(exc.orig).lower()
    except Exception:
        msg = str(exc).lower()
    return any(h in msg for h in ("unique", "duplicate", "uq_", "already exists", "unique constraint"))

# 🔴 404, 403, 401 و ... → خطاهای HTTP عمومی
async def handle_http_exception(request: Request, exc: StarletteHTTPException):
    return create_response(
        status_code=exc.status_code,
        message=f"{exc.detail or 'خطای HTTP رخ داده است.'}",
        data={},
    )

# ❌ خطای اعتبارسنجی پارامترها / body → 422
async def handle_validation_error(request: Request, exc: RequestValidationError):
    return create_response(
        status_code=HTTP_422_UNPROCESSABLE_ENTITY,
        message="ورودی‌های شما نامعتبر است",
        data={"errors": exc.errors()}
    )

# ❌ خطای دیتابیس → 409 برای unique | در غیر اینصورت 400
async def handle_integrity_error(request: Request, exc: IntegrityError):
    if _is_unique_violation(exc):
        return create_response(
            status_code=HTTP_409_CONFLICT,
            message="رکورد تکراری است (unique constraint).",
            data={}
        )
    return create_response(
        status_code=HTTP_400_BAD_REQUEST,
        message="خطای ناسازگاری داده یا قیود پایگاه داده",
        data={}
    )

# 🔐 خطای JWT (توکن نامعتبر یا منقضی شده) → 401
async def handle_jwt_error(request: Request, exc: JWTError):
    msg = "توکن معتبر نیست یا منقضی شده است" if not isinstance(exc, ExpiredSignatureError) else "توکن شما منقضی شده است."
    return create_response(
        status_code=HTTP_401_UNAUTHORIZED,
        message=msg,
        data={}
    )

# 🟦 خطای بیزنسی AppException (اختیاری)
async def handle_app_exception(request: Request, exc: AppException):
    return create_response(
        status_code=exc.status_code,
        message=exc.message,
        data={"errors": exc.errors, **(exc.data or {})} if exc.errors else (exc.data or {})
    )

# 🧨 خطاهای پیش‌بینی‌نشده → 500
async def handle_general_exception(request: Request, exc: Exception):
    return create_response(
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
        message="خطای غیرمنتظره‌ای رخ داد. لطفاً بعداً تلاش کنید.",
        data={}
    )
