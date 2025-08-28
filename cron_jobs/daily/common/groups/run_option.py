# cron_jobs/daily/groups/run_option.py
from cron_jobs.daily.common.base_updater import run_group
if __name__ == "__main__":
    run_group("option", "daily_option")