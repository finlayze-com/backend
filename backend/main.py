from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.api import sankey  # قبل از include
from backend.api import treemap
from backend.api import orderbook
from backend.api import OrderbookData
from backend.api import real_money_flow
from backend.api import candlestick
from backend.api import metadata  # اضافه کن
from backend.users.routes import router as user_router


print("🚀 [MAIN] FastAPI is loading main.py")


app = FastAPI(
    title="Full Financial API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(user_router, prefix="/auth", tags=["auth & users"])
app.include_router(metadata.router, prefix="/api")  # اضافه کن
app.include_router(sankey.router, prefix="/sankey")
app.include_router(treemap.router, prefix="/api")
app.include_router(orderbook.router, prefix="/api")
app.include_router(real_money_flow.router, prefix="/api")
app.include_router(OrderbookData.router, prefix="/api")
app.include_router(candlestick.router, prefix="/api")

@app.get("/ping")
def ping():
    return {"message": "pong"}
