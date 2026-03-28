"""Scheduler - runs the analysis pipeline on a cron schedule."""

import logging

import yaml

from main import load_config, run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("scheduler")


def start_scheduler():
    """Start the APScheduler-based cron scheduler."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    config = load_config()
    sched_config = config.get("schedule", {})

    if not sched_config.get("enabled", False):
        logger.info("Scheduler is disabled in config. Running once immediately.")
        run_pipeline(config)
        return

    cron_expr = sched_config.get("cron_expression", "30 15 * * 1-5")
    parts = cron_expr.split()
    minute, hour, day, month, dow = parts[0], parts[1], parts[2], parts[3], parts[4]

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_pipeline,
        "cron",
        args=[config],
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=dow,
    )

    logger.info(f"Scheduler started. Cron: {cron_expr}")
    logger.info("Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    start_scheduler()
