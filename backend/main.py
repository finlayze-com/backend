from fastapi import FastAPI,Request
from fastapi.middleware.cors import CORSMiddleware
from backend.middleware.middleware_auth import AuthMiddleware
from backend.middleware.middleware_auth import AuthMiddleware
from backend.utils.logger import logger  # ← لاگر سفارشی



# 💼 APIهای مالی
from backend.api import sankey, treemap, orderbook, OrderbookData, real_money_flow, candlestick, metadata

# 👤 ماژول‌های کاربری
from backend.users.routes import (
    auth,
    subscriptions,
    roles,
    permissions,
    users,
    usersubscription
)

print("🚀 [MAIN] FastAPI is loading main.py")

app = FastAPI(
    title="Full Financial API",
    version="1.0.0"
)

# ✅ middleware برای لاگ‌گیری همه درخواست‌ها
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"➡️ Request: {request.method} {request.url}")
    try:
        response = await call_next(request)
    except Exception as e:
        logger.exception("🔥 خطای ناشناخته هنگام پردازش درخواست")
        raise e
    logger.info(f"⬅️ Response: status_code={response.status_code}")
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

# 🔍 health check
@app.get("/ping")
def ping():
    return {"message": "pong"}
