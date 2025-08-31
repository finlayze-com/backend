# cron_jobs/daily/common/loaders.py
import jdatetime
import pandas as pd
import finpy_tse as fps

def convert_jalali_to_gregorian(jdate_str: str) -> str | None:
    try:
        y, m, d = map(int, jdate_str.split("-"))
        return jdatetime.date(y, m, d).togregorian().strftime("%Y-%m-%d")
    except Exception:
        return None

def get_price_history(stock: str) -> pd.DataFrame | None:
    """برگشت df با ستون‌های استاندارد finpy_tse و index=تاریخ جلالی (yyyy-mm-dd)"""
    df = fps.Get_Price_History(stock=stock, ignore_date=True, adjust_price=True, show_weekday=True)
    if df is None or df.empty:
        return None

    df = df.copy()
    df["j_date"] = df.index.astype(str)
    df["gregorian_date"] = df["j_date"].map(convert_jalali_to_gregorian)
    df["gregorian_date"] = pd.to_datetime(df["gregorian_date"])
    return df
