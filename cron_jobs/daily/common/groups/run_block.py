# cron_jobs/daily/groups/run_block.py
from cron_jobs.daily.common.base_updater import run_group
if __name__ == "__main__":
    run_group("Block", "daily_block")