from jose import jwt

SECRET_KEY = "Afiroozi12!@^erySecretKey9876*"
ALGORITHM = "HS256"

data = {
    "sub": "1",
    "roles": ["superadmin"],
    "permissions": [],
    "features": {
        "Treemap": True,
        "OrderBook": True,
        "Candlestick": True
    },
    "exp": 1753703952  # هرچیزی، فقط برای تست
}

token = jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)
print("Generated token:", token)

decoded = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
print("Decoded payload:", decoded)
