# cron_jobs/daily/groups/run_fund_leverage.py
from cron_jobs.daily.common.base_updater import run_group
if __name__ == "__main__":
    run_group("fund_leverage", "daily_fund_leverage")
