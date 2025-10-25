from fastapi import FastAPI,Request
from fastapi.middleware.cors import CORSMiddleware

from backend.middleware.middleware_auth import AuthMiddleware
from backend.utils.logger import logger  # ← لاگر سفارشی
from dotenv import load_dotenv
import os
# 📦 Exception handlers
from backend.utils.Exception_Handler import (
    handle_http_exception,
    handle_validation_error,
    handle_integrity_error,
    handle_jwt_error,
    handle_general_exception,
    handle_app_exception
)
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import IntegrityError
from jose.exceptions import JWTError
from starlette.exceptions import HTTPException as StarletteHTTPException
from backend.utils.exceptions import AppException


load_dotenv(dotenv_path=".env")
print("🧪 Loaded SECRET_KEY from middleware:", repr(os.getenv("SECRET_KEY")))



# 💼 APIهای مالی
from backend.api import sankey, treemap, orderbook, OrderbookData, real_money_flow, candlestick, metadata,liquidity_weekly

# 👤 ماژول‌های کاربری
from backend.users.routes import (
    auth,
    subscriptions,
    roles,
    permissions,
    users,
    usersubscription,
    inscodeid
)

print("🚀 [MAIN] FastAPI is loading main.py")

app = FastAPI(
    title="Full Financial API",
    version="1.0.0"
)

# ⛑️ ثبت هندلرهای اختصاصی برای خطاها
app.add_exception_handler(StarletteHTTPException, handle_http_exception)
app.add_exception_handler(RequestValidationError, handle_validation_error)
app.add_exception_handler(IntegrityError, handle_integrity_error)
app.add_exception_handler(JWTError, handle_jwt_error)
app.add_exception_handler(AppException, handle_app_exception)      # ← اگر قدم 0
app.add_exception_handler(Exception, handle_general_exception)


# ✅ middleware برای لاگ‌گیری همه درخواست‌ها
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"➡️ Request: {request.method} {request.url}")
    try:
        response = await call_next(request)
    except Exception as e:
        logger.exception(" خطای ناشناخته هنگام پردازش درخواست")
        raise e
    logger.info(f" Response: status_code={response.status_code}")
    return response

# 🛡️ احراز هویت کاستوم
app.add_middleware(AuthMiddleware)  # 👈 اینو اضافه کن قبل از include_router


# 🌐 فعال‌سازی CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ✅ اضافه‌کردن routeهای auth و کاربران
app.include_router(auth.router, tags=["🔐 Auth"])  # 🔑 login/register
app.include_router(usersubscription.router, prefix="/auth", tags=["📊 User Subscriptions"]) # user_subscription management
app.include_router(subscriptions.router, tags=["📦 Subscriptions"]) # 💳 plans
app.include_router(roles.router, tags=["🧩 Roles"]) # 🎭 role
app.include_router(permissions.router, tags=["✅ Permissions"]) # ✅ permissions
app.include_router(users.router, tags=["👥 Users"]) # 👤 user management

# ✅ اضافه‌کردن APIهای مالی (قبلی)
app.include_router(metadata.router, prefix="/api")
app.include_router(sankey.router, prefix="/sankey")
app.include_router(treemap.router, prefix="/api")
app.include_router(orderbook.router, prefix="/api")
app.include_router(real_money_flow.router, prefix="/api")
app.include_router(OrderbookData.router, prefix="/api")
app.include_router(candlestick.router, prefix="/api")

app.include_router(liquidity_weekly.router, prefix="/api")  # ✅ درست


app.include_router(inscodeid.router, prefix="/inscodeid")

# 🔍 health check
@app.get("/ping")
def ping():
    return {"message": "pong"}
