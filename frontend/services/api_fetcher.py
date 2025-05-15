import requests
import pandas as pd

BASE_URL = "http://127.0.0.1:8000"

def get_sector_net_real_flow():
    try:
        response = requests.get("http://127.0.0.1:8000/sankey/sector")
        response.raise_for_status()
        data = response.json()

        # برای مثال: استخراج nodes و links
        nodes = pd.DataFrame(data["series"]["data"])
        links = pd.DataFrame(data["series"]["links"])

        return {"nodes": nodes, "links": links}

    except Exception as e:
        print("❌ خطا در دریافت داده از FastAPI:", e)
        return {"nodes": pd.DataFrame(), "links": pd.DataFrame()}


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


BASE_URL = "http://127.0.0.1:8000/api"

def fetch_real_money_flow(timeframe="daily", level="sector", sector=None, currency="rial"):
    try:
        params = {
            "timeframe": timeframe,
            "level": level,
            "currency": currency
        }
        if level == "stock_ticker" and sector:
            params["sector"] = sector

        res = requests.get(f"{BASE_URL}/real-money-flow/timeseries", params=params)
        res.raise_for_status()
        return pd.DataFrame(res.json())
    except Exception as e:
        print("❌ خطا در دریافت داده روند حقیقی‌ها از API:", e)
        return pd.DataFrame()

BASE_URL = "http://127.0.0.1:8000/api"

def fetch_OrderbookData(mode="sector", sector=None):
    """
    دریافت داده تایم‌سری خالص سفارشات برای نمودار LineChart
    """
    try:
        params = {"mode": mode}
        if mode == "intra-sector" and sector:
            params["sector"] = sector

        res = requests.get(f"{BASE_URL}/orderbookdata/timeseries", params=params)
        res.raise_for_status()
        return pd.DataFrame(res.json())
    except Exception as e:
        print("❌ خطا در دریافت داده OrderbookData از API:", e)
        return pd.DataFrame()

def get_intra_sector_net_real_flow(sector):
    try:
        response = requests.get("http://127.0.0.1:8000/sankey/intra-sector", params={"sector": sector})
        response.raise_for_status()
        data = response.json()
        nodes = pd.DataFrame(data["series"]["data"])
        links = pd.DataFrame(data["series"]["links"])
        return {"nodes": nodes, "links": links}
    except Exception as e:
        print("❌ خطا در intra-sector:", e)
        return {"nodes": pd.DataFrame(), "links": pd.DataFrame()}
