# cron_jobs/daily/groups/run_fund_fixincome.py
from cron_jobs.daily.common.base_updater import run_group
if __name__ == "__main__":
    run_group("fund_fixincome", "daily_fund_fixincome")
