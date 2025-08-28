from cron_jobs.daily.common.base_updater import run_group

if __name__ == "__main__":
    run_group("retail", "daily_retail")
    # cron_jobs/daily/groups/run_retail.py
