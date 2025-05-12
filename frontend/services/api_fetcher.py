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


BASE_URL = "http://127.0.0.1:8000"

def fetch_treemap_data(timeframe="daily", size_mode="market_cap", sector=None, include_etf=True):
    try:
        params = {
            "size_mode": size_mode,
            "sector": sector,
            "include_etf": include_etf
        }
        res = requests.get(f"{BASE_URL}/treemap/{timeframe}", params=params)
        res.raise_for_status()
        return pd.DataFrame(res.json())
    except Exception as e:
        print("❌ خطا در دریافت داده Treemap از API:", e)
        return pd.DataFrame()


BASE_URL = "http://127.0.0.1:8000/api"

def fetch_orderbook_timeseries(mode="sector", sector=None):
    try:
        params = {"mode": mode}
        if mode == "intra-sector" and sector:
            params["sector"] = sector

        res = requests.get(f"{BASE_URL}/orderbook/timeseries", params=params)
        res.raise_for_status()
        return pd.DataFrame(res.json())
    except Exception as e:
        print("❌ خطا در دریافت داده Orderbook از API:", e)
        return pd.DataFrame()
