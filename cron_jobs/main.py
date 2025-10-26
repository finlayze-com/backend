# -*- coding: utf-8 -*-
"""
APScheduler Main Runner
- Live jobs: every minute between 09:00 and 12:30 (Sat..Wed)
- Nightly batch: exactly at 19:00 (Sat..Wed)
- Logs to cron_jobs/logs/scheduler.log
- Respects APP_TZ env (default: Asia/Tehran)

Test locally:
    .venv/bin/python -m cron_jobs.main
"""

import os
import sys
import shlex
import time
import signal
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Tuple, Optional, Callable

# ---- Third-party (APScheduler) ----
try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
except Exception as e:
    print("[FATAL] You must install APScheduler: pip install apscheduler", file=sys.stderr)
    raise

# ---- Timezone (ZoneInfo built-in) ----
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    # fallback to pytz if needed
    try:
        from pytz import timezone as ZoneInfo  # type: ignore
    except Exception:
        print("[FATAL] Need Python 3.9+ (zoneinfo) or pytz installed.", file=sys.stderr)
        raise

# ---- (Optional) dotenv ----
try:
    from dotenv import load_dotenv
    _HAS_DOTENV = True
except Exception:
    _HAS_DOTENV = False

# ======================================================================================
# Paths & Constants
# ======================================================================================

# file location: cron_jobs/main.py  → project root = parent of "cron_jobs"
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent

LOG_DIR = PROJECT_ROOT / "cron_jobs" / "logs"
LOG_FILE = LOG_DIR / "scheduler.log"

# Default timezone (override via APP_TZ in env / .env)
APP_TZ_NAME = os.getenv("APP_TZ", "Asia/Tehran")
try:
    APP_TZ = ZoneInfo(APP_TZ_NAME)  # ZoneInfo or pytz tzinfo
except Exception:
    print(f"[WARN] Invalid APP_TZ='{APP_TZ_NAME}', falling back to 'Asia/Tehran'")
    APP_TZ_NAME = "Asia/Tehran"
    APP_TZ = ZoneInfo(APP_TZ_NAME)

# Live tasks (you asked to run by file: python cron_jobs/livedata/run_live_*.py)
LIVE_TASKS: List[Tuple[str, Path]] = [
    ("live_saver",     PROJECT_ROOT / "cron_jobs" / "livedata" / "run_live_saver.py"),
    ("live_orderbool", PROJECT_ROOT / "cron_jobs" / "livedata" / "run_live_orderbool.py"),
]

# Nightly modules to run with -m (without ".py")
NIGHTLY_MODULES: List[Tuple[str, str]] = [
    ("dollar",                "cron_jobs.otherimportantFile.dollar"),
    ("run_saham",             "cron_jobs.daily.common.groups.run_saham"),
    ("update_daily_haghighi", "cron_jobs.daily.update_daily_haghighi"),
    ("run_saham_ind",         "cron_jobs.daily.common.groups.run_saham_ind"),
]

# Days of week: Sat..Wed  (Linux cron usually: 0/7=Sun, 6=Sat. APScheduler uses names)
DOW_STR = "sat,sun,mon,tue,wed"

# ======================================================================================
# Logging
# ======================================================================================

logger = logging.getLogger("oscmap.scheduler")
logger.setLevel(logging.INFO)

def _setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    # Avoid duplicate handlers when reloading
    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        logger.addHandler(fh)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(ch)

# ======================================================================================
# Utilities to run commands/scripts
# ======================================================================================

def _python_executable() -> str:
    """Return current interpreter (typically .venv/bin/python)."""
    return sys.executable

def run_python_file(py_path: Path, name: Optional[str] = None) -> int:
    """
    Run a standalone python file with the current interpreter.
    Returns process returncode.
    """
    task_name = name or py_path.stem
    if not py_path.exists():
        logger.error(f"❌ [{task_name}] file not found: {py_path}")
        return 127

    cmd_list = [_python_executable(), str(py_path)]
    cmd_str = " ".join(shlex.quote(c) for c in cmd_list)
    logger.info(f"▶️  [{task_name}] FILE start: {cmd_str}")

    # Use subprocess.run with realtime stdout/stderr piping (simple version)
    import subprocess
    proc = subprocess.Popen(cmd_list, cwd=str(PROJECT_ROOT),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, encoding="utf-8", bufsize=1)

    # stream logs
    assert proc.stdout is not None
    for line in proc.stdout:
        logger.info(f"[{task_name}] {line.rstrip()}")

    proc.wait()
    rc = proc.returncode
    if rc == 0:
        logger.info(f"✅ [{task_name}] FILE done (rc=0)")
    else:
        logger.error(f"❌ [{task_name}] FILE failed (rc={rc})")

    return rc

def run_python_module(module_path: str, name: Optional[str] = None) -> int:
    """
    Run a python module with '-m' using the current interpreter.
    Returns process returncode.
    """
    task_name = name or module_path
    cmd_list = [_python_executable(), "-m", module_path]
    cmd_str = " ".join(shlex.quote(c) for c in cmd_list)
    logger.info(f"▶️  [{task_name}] MODULE start: {cmd_str}")

    import subprocess
    proc = subprocess.Popen(cmd_list, cwd=str(PROJECT_ROOT),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, encoding="utf-8", bufsize=1)

    assert proc.stdout is not None
    for line in proc.stdout:
        logger.info(f"[{task_name}] {line.rstrip()}")

    proc.wait()
    rc = proc.returncode
    if rc == 0:
        logger.info(f"✅ [{task_name}] MODULE done (rc=0)")
    else:
        logger.error(f"❌ [{task_name}] MODULE failed (rc={rc})")
    return rc

def _make_file_task(name: str, path: Path) -> Callable[[], None]:
    """Wrap a file-runner as an APScheduler job function with error handling."""
    def _job():
        try:
            run_python_file(path, name=name)
        except Exception as e:
            logger.exception(f"❌ [{name}] crashed with exception: {e}")
    return _job

# ======================================================================================
# Scheduling
# ======================================================================================

def schedule_live_minutely_window(sched: BlockingScheduler):
    """
    Every minute 09:00–11:59 and 12:00–12:30 on Sat..Wed.
    Prevent overlaps with max_instances=1.
    """
    for name, path in LIVE_TASKS:
        job_fn = _make_file_task(name, path)

        # 09:00..11:59 (every minute)
        sched.add_job(
            job_fn,
            CronTrigger(minute="*", hour="9-11", day_of_week=DOW_STR, timezone=APP_TZ),
            id=f"live_{name}_morning",
            replace_existing=True,
            misfire_grace_time=10 * 60,
            max_instances=1,
            coalesce=True,
        )
        # 12:00..12:30 (minutes 0..30)
        sched.add_job(
            job_fn,
            CronTrigger(minute="0-30", hour="12", day_of_week=DOW_STR, timezone=APP_TZ),
            id=f"live_{name}_noon",
            replace_existing=True,
            misfire_grace_time=10 * 60,
            max_instances=1,
            coalesce=True,
        )
        logger.info(f"⏰ [{name}] scheduled @ 09:00–12:30 ({DOW_STR})")

def nightly_batch_1900():
    """
    Run the 4 nightly modules in sequence at exactly 19:00.
    Continue even if one fails.
    """
    logger.info("🌙 NIGHTLY(19:00) START")
    for n, m in NIGHTLY_MODULES:
        try:
            rc = run_python_module(m, name=n)
            if rc != 0:
                logger.error(f"[WARN] nightly step failed: {n} (rc={rc})")
        except Exception as e:
            logger.exception(f"❌ nightly step crashed: {n} ({e})")
    logger.info("✅ NIGHTLY(19:00) END")

def schedule_nightly_batch(sched: BlockingScheduler):
    sched.add_job(
        nightly_batch_1900,
        CronTrigger(hour=19, minute=0, day_of_week=DOW_STR, timezone=APP_TZ),
        id="nightly_batch_1900",
        replace_existing=True,
        misfire_grace_time=30 * 60,
        max_instances=1,
        coalesce=True,
    )
    logger.info("⏰ [nightly_batch_1900] scheduled @ 19:00 ({})".format(DOW_STR))

# ======================================================================================
# Main
# ======================================================================================

def main():
    # 1) load .env if exists (optional)
    if _HAS_DOTENV:
        env_path = PROJECT_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"🔧 .env loaded from {env_path}")

    # 2) logging
    _setup_logging()
    logger.info("========== Scheduler boot ==========")
    logger.info(f"PROJECT_ROOT = {PROJECT_ROOT}")
    logger.info(f"APP_TZ       = {APP_TZ_NAME}")

    # 3) Basic checks
    for name, path in LIVE_TASKS:
        if not path.exists():
            logger.warning(f"⚠️  Live script missing: [{name}] {path}")

    # 4) Scheduler
    sched = BlockingScheduler(timezone=APP_TZ)
    # Live window 09:00–12:30
    schedule_live_minutely_window(sched)
    # Nightly 19:00
    schedule_nightly_batch(sched)

    # 5) handle signals for graceful shutdown
    def _graceful(signum, frame):
        logger.info(f"🛑 Caught signal {signum}; shutting down scheduler...")
        try:
            sched.shutdown(wait=False)
        finally:
            sys.exit(0)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _graceful)
        except Exception:
            pass

    # 6) start
    try:
        logger.info("🚀 Scheduler started.")
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Scheduler stopped.")
    except Exception:
        logger.exception("💥 Scheduler crashed unexpectedly.")

if __name__ == "__main__":
    main()
