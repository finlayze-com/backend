from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request
from starlette.responses import JSONResponse
from jose import jwt, JWTError
from backend.users.models import get_user_by_id, get_user_permissions
from backend.users.routes.auth import SECRET_KEY, ALGORITHM

# مسیرهایی که نیاز به احراز هویت ندارند
PUBLIC_PATHS = ["/docs",
     "/docs/", "/openapi.json", "/favicon.ico", "/ping", "/static",
    "/login", "/register"
]

# تابع بررسی عمومی بودن مسیر
def is_public_path(path: str) -> bool:
    return (
        path in PUBLIC_PATHS or
        path.startswith("/static") or
        path.startswith("/docs")
    )

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        print(f"📥 مسیر دریافت‌شده: {path}")

        # اگر مسیر عمومی است، بررسی توکن نیاز نیست
        if is_public_path(path):
            print("🟢 مسیر عمومی تشخیص داده شد → بدون نیاز به توکن")
            return await call_next(request)

        # دریافت توکن
        auth_header = request.headers.get("authorization") or request.headers.get("authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            print("❌ هدر Authorization ارسال نشده یا اشتباه است")
            return JSONResponse(status_code=401, content={"detail": "توکن ارسال نشده"})

        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            user_id = int(payload.get("sub"))  # مطمئن شو 'sub' در توکن ذخیره میشه
        except JWTError as e:
            print("❌ خطا در دیکد کردن توکن:", e)
            return JSONResponse(status_code=401, content={"detail": "توکن نامعتبر است"})

        # بررسی وجود کاربر
        user = await get_user_by_id(user_id)
        if not user:
            return JSONResponse(status_code=401, content={"detail": "کاربر یافت نشد"})

        # دریافت سطح دسترسی
        permissions = await get_user_permissions(user_id)
        request.state.user = user
        request.state.permissions = permissions

        return await call_next(request)
