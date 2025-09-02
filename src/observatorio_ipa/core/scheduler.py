# scheduler.py
from __future__ import annotations
import logging, signal, sys, os
from pathlib import Path
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from observatorio_ipa.core import config, cli, setup_db
from observatorio_ipa.core.config import LOGGER_NAME, Settings
from observatorio_ipa.core.scripting import parse_to_bool
from observatorio_ipa.core.workflows.automation.orchestration import (
    auto_job_init,
    auto_job_orchestration,
)
from observatorio_ipa.utils.logs import init_logging_config

logger = logging.getLogger(LOGGER_NAME)


def parse_cron_expr(expr: str) -> CronTrigger:
    """
    Accepts standard 5-field crontab: 'min hour doy month dow'
    """
    fields = expr.split()
    if len(fields) != 5:
        raise ValueError(f"Invalid cron expression (expected 5 fields): {expr}")
    minute, hour, day, month, dow = fields
    return CronTrigger(minute=minute, hour=hour, day=day, month=month, day_of_week=dow)


def _job_create(settings: Settings) -> None:
    logger.info("create_jobs: start")
    auto_job_init(settings)
    logger.info("create_jobs: end")


def _job_poll(settings: Settings) -> None:
    logger.info("poll_and_orchestrate: start")
    auto_job_orchestration(settings)
    logger.info("poll_and_orchestrate: end")


def main():
    # ---------- Initialization ----------
    # Parse CLI args
    cli_args = cli.parse_cli_args()

    # Load settings
    toml_file = cli_args.get("toml_file", "")
    toml_file = toml_file.strip("'\"")  # clean quotes if any
    if not toml_file:
        raise SystemExit(
            "Config error: IPA_CONFIG_TOML env var or --toml argument required."
        )

    toml_file = Path(toml_file)
    if not toml_file.is_file():
        raise SystemExit(f"Config error: settings file not found: {toml_file}")

    runtime_settings = config.load_settings_from_toml(toml_file)

    # Create logger (Currently file, and Stream if Containerized)
    logger = init_logging_config(
        runtime_settings.app.logging,
        parse_to_bool(os.getenv("IPA_CONTAINERIZED", "False")),
    )

    # ---------- Setting DB ----------
    db_path_ = runtime_settings.app.automation.db.db_path
    db_path = Path(db_path_).expanduser().resolve()
    # Use default DB name if only providing path
    if db_path.suffix == "":
        db_path = (db_path / setup_db.DEFAULT_DB_NAME).resolve()

    if not db_path.is_file():
        logger.warning(
            f"DB file not found at {db_path_}. A new DB will be created as first run."
        )
        logger.info(f"Creating new DB at {db_path_}...")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        setup_db.create_schema(db_path)

    # ---------- Creating Scheduler ----------
    # Create Scheduler
    tz_str = runtime_settings.app.automation.timezone
    try:
        tz = ZoneInfo(tz_str)
    except Exception as e:
        raise SystemExit(f"Config error: invalid timezone '{tz_str}': {e}")

    sched = BlockingScheduler(timezone=tz_str, logger=logger)

    # --- Create job scheduling
    job_cron = runtime_settings.app.automation.daily_job.cron
    if job_cron:
        cron_trigger = parse_cron_expr(job_cron)
    else:
        raise SystemExit("Config error: cron for Job execution must be set.")
    sched.add_job(
        func=_job_create,
        trigger=cron_trigger,
        kwargs={"settings": runtime_settings},
        id="auto_daily_job",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # --- Poll scheduling
    interval_minutes = (
        runtime_settings.app.automation.orchestration_job.interval_minutes
    )

    sched.add_job(
        func=_job_poll,
        trigger=IntervalTrigger(minutes=interval_minutes),
        kwargs={"settings": runtime_settings},
        id="auto_orchestration",
        max_instances=1,  # prevent overlapping polls from APScheduler side
        coalesce=True,  # if we fall behind, run once (not catch up N times)
        misfire_grace_time=60,  # seconds grace to run if missed (e.g., restart)
    )

    # graceful shutdown on SIGTERM/SIGINT
    def shutdown(signum, frame):
        logger.info(f"Signal {signum} received, shutting down scheduler…")
        sched.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    logger.info("Starting APScheduler…")
    sched.start()


if __name__ == "__main__":
    main()
