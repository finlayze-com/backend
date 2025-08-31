# cron_jobs/daily/groups/run_fund_stock.py
from cron_jobs.daily.common.base_updater import run_group
if __name__ == "__main__":
    run_group("fund_stock", "daily_fund_stock")