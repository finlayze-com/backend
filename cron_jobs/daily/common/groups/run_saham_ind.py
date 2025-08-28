# cron_jobs/daily/groups/run_ind_saham.py
from cron_jobs.daily.common.base_indicator import build_indicators_for_table

if __name__ == "__main__":
    # منبع: daily_stock_data  ← مقصد: daily_indicators
    build_indicators_for_table("daily_stock_data", "daily_indicators",insert_mode="upsert")
