import requests
import pandas as pd

BASE_URL = "http://127.0.0.1:8000"

def get_sector_net_real_flow():
    try:
        response = requests.get(f"{BASE_URL}/sankey/sector")
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)
    except Exception as e:
        print("❌ خطا در دریافت داده از FastAPI:", e)
        return pd.DataFrame()
