# cron_jobs/daily/groups/run_fund_segment.py
from cron_jobs.daily.common.base_updater import run_group
if __name__ == "__main__":
    run_group("fund_segment", "daily_fund_segment")