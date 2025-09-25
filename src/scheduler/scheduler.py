"""Factory helpers for building and populating the shared APScheduler instance."""

from pathlib import Path
from typing import Any, Dict

import yaml
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from configs.env_config import Env
from scheduler.job_runner import run_job
from utils.logger_factory import log_exception

DEFAULTS = {
    "coalesce": True,
    "max_instances": 1,
    "misfire_grace_time": 60,
}

UTC = ZoneInfo("UTC")


def build_scheduler() -> AsyncIOScheduler:
    """Create an ``AsyncIOScheduler`` backed by the configured SQLAlchemy store."""
    jobstores = {"default": SQLAlchemyJobStore(url=Env.SQLALCHEMY_URL)}
    scheduler = AsyncIOScheduler(
        timezone=UTC,
        jobstores=jobstores,
        job_defaults=DEFAULTS,
    )
    return scheduler


def load_jobs_from_yaml(
    scheduler: AsyncIOScheduler,
    path: Path,
    tz: ZoneInfo = UTC,
    etl_logger=None,
) -> None:
    """Synchronize scheduler jobs from a YAML configuration file.

    :param scheduler: Target ``AsyncIOScheduler`` instance to populate.
    :param path: Path to the YAML file describing jobs.
    :param tz: Default timezone applied to newly created triggers.
    :param etl_logger: Logger used for status and error reporting.
    """
    if not path.exists():
        etl_logger.warning(f"Jobs config not found: {path}")
        return

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    for item in data.get("jobs", []):
        try:
            job_id: str = item["id"]
            func_spec: str = item["func"]
            trigger_type = item.get("trigger", "cron")
            kwargs: Dict[str, Any] = item.get("kwargs", {})

            try:
                scheduler.remove_job(job_id)
            except JobLookupError:
                pass

            job_defaults = {
                "id": job_id,
                "replace_existing": True,
                "misfire_grace_time": item.get("misfire_grace_time", DEFAULTS["misfire_grace_time"]),
                "max_instances": item.get("max_instances", DEFAULTS["max_instances"]),
                "coalesce": item.get("coalesce", DEFAULTS["coalesce"]),
            }
 
            if trigger_type == "cron":
                #trigger = CronTrigger.from_crontab(item["cron"], timezone=UTC)
                trigger = CronTrigger(second=item.get("second", "*"),
                    minute=item.get("minute", "*"),
                    hour=item.get("hour", "*"),
                    day=item.get("day", "*"),
                    month=item.get("month", "*"),
                    day_of_week=item.get("day_of_week", "*"),
                    timezone=tz)
            elif trigger_type == "interval":
                every = item.get("every", {"minutes": 5})
                trigger = IntervalTrigger(timezone=UTC, **every)
            elif trigger_type == "date":
                trigger = DateTrigger(run_date=item["run_date"], timezone=UTC)
            else:
                raise ValueError(f"Unsupported trigger: {trigger_type}")

            scheduler.add_job(
                func=run_job,
                trigger=trigger,
                kwargs={
                    "func_spec": func_spec,
                    "job_id": job_id,
                    "kwargs": kwargs,
                    # "level": LogLevel.DEBUG,  # 可調
                },
                **job_defaults,
            )
            etl_logger.info(f"Registered job: {job_id} ({trigger_type}) -> {func_spec}")
        except Exception as e:
            log_exception(etl_logger, e, context=f"load_jobs_from_yaml[{item.get('id','?')}]")
    

    for j in scheduler.get_jobs():
        try:
            f = j.func
            etl_logger.info(f"job={j.id} func={f.__module__}.{f.__qualname__} kwargs={j.kwargs}")
        except Exception as e:
            etl_logger.error(f"inspect job {j.id} failed: {e}")
