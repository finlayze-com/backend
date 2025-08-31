# cron_jobs/daily/groups/run_commodity.py
from cron_jobs.daily.common.base_updater import run_group
if __name__ == "__main__":
    run_group("commodity", "daily_commodity")