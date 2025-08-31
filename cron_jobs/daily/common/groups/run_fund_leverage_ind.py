# cron_jobs/daily/groups/run_ind_saham.py
from cron_jobs.daily.common.base_indicator import build_indicators_for_table

if __name__ == "__main__":
    build_indicators_for_table("daily_fund_leverage", "daily_indicators_fund_leverage",insert_mode="upsert")
