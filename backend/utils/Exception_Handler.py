from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import IntegrityError
from jose.exceptions import JWTError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_422_UNPROCESSABLE_ENTITY,
    HTTP_500_INTERNAL_SERVER_ERROR,
)


# ✅ تابع استاندارد ساخت response
def create_response(status_code: int, message: str, data: dict = None):
    return JSONResponse(
        status_code=status_code,
        content={
            "status_code": status_code,
            "message": message,
            "data": data or {},
        }
    )


# 🔴 404, 403, 401 و ... → خطاهای HTTP
async def handle_http_exception(request: Request, exc: StarletteHTTPException):
    return create_response(
        status_code=exc.status_code,
        message=f"❌ {exc.detail or 'خطای HTTP رخ داده است.'}"
    )


# ❌ خطای اعتبارسنجی پارامترها / body
async def handle_validation_error(request: Request, exc: RequestValidationError):
    return create_response(
        status_code=HTTP_422_UNPROCESSABLE_ENTITY,
        message="ورودی‌های شما نامعتبر است",
        data={"errors": exc.errors()}
    )


# ❌ خطای دیتابیس (مثلاً تکراری بودن ایمیل)
async def handle_integrity_error(request: Request, exc: IntegrityError):
    return create_response(
        status_code=HTTP_400_BAD_REQUEST,
        message="خطای تکراری بودن یا ناسازگاری اطلاعات در پایگاه داده"
    )


# 🔐 خطای JWT (توکن نامعتبر یا منقضی شده)
async def handle_jwt_error(request: Request, exc: JWTError):
    return create_response(
        status_code=HTTP_401_UNAUTHORIZED,
        message="توکن معتبر نیست یا منقضی شده است"
    )


# 🧨 خطاهای پیش‌بینی‌نشده
async def handle_general_exception(request: Request, exc: Exception):
    return create_response(
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
        message="خطای غیرمنتظره‌ای رخ داد. لطفاً بعداً تلاش کنید."
    )
