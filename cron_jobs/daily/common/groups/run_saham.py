# cron_jobs/daily/groups/run_saham.py
from cron_jobs.daily.common.base_updater import run_group

if __name__ == "__main__":
    run_group("saham", "daily_stock_data")
