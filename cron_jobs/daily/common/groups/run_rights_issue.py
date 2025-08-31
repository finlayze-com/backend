# cron_jobs/daily/groups/run_rights_issue.py
from cron_jobs.daily.common.base_updater import run_group

if __name__ == "__main__":
    run_group("rights_issue", "daily_rights_issue")











