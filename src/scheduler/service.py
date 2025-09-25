"""Scheduler service with job monitoring, manual triggers, and streaming."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Deque, Dict, Iterable, Optional

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_JOB_SUBMITTED,
    JobEvent,
    JobExecutionEvent,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import STATE_PAUSED, STATE_RUNNING, STATE_STOPPED
from apscheduler.triggers.date import DateTrigger

from model.scheduler import JobRunRecord, JobStats
from scheduler.scheduler import UTC, build_scheduler, load_jobs_from_yaml
from utils.logger.logger import Logger
from utils.logger_factory import EnhancedLoggerFactory


class SchedulerMonitor:
    """APScheduler listener that tracks per-job execution data."""

    def __init__(self, *, history_size: int = 50, on_event=None) -> None:
        """Initialise the monitor with history capacity and an event hook.

        :param history_size: Maximum events retained per job in memory.
        :param on_event: Optional callback invoked with serialisable payloads.
        """
        self._lock = Lock()
        self._stats: Dict[str, JobStats] = {}
        self._inflight: Dict[str, datetime] = {}
        self._history_size = history_size
        self._on_event = on_event

    def _initial_stats(self) -> JobStats:
        """Return an empty ``JobStats`` instance with the configured maxlen."""
        stats = JobStats()
        stats.history = deque(maxlen=self._history_size)
        return stats

    def default_stats(self) -> Dict[str, Any]:
        """Provide a serialisable default stats structure for new jobs."""
        return _serialize_stats(self._initial_stats())

    def handle_event(self, event: JobEvent) -> None:
        """Consume an APScheduler event and update the in-memory stats."""
        code = event.code
        now = datetime.now(tz=UTC)

        with self._lock:
            stats = self._stats.setdefault(event.job_id, self._initial_stats())

            if code & EVENT_JOB_SUBMITTED:
                stats.total_runs += 1
                stats.last_event = "submitted"
                stats.last_scheduled_at = getattr(event, "scheduled_run_time", None)
                stats.last_started_at = now
                stats.history.append(
                    JobRunRecord(
                        event="submitted",
                        recorded_at=now,
                        scheduled_at=stats.last_scheduled_at,
                    )
                )
                self._inflight[event.job_id] = now
                self._emit(event.job_id, stats)
                return

            if code & EVENT_JOB_EXECUTED:
                stats.total_success += 1
                stats.last_event = "success"
                start = self._inflight.pop(event.job_id, stats.last_started_at)
                stats.last_finished_at = now
                stats.last_duration_ms = _calc_duration_ms(start, now)
                stats.last_error = None
                stats.history.append(
                    JobRunRecord(
                        event="success",
                        recorded_at=now,
                        scheduled_at=getattr(event, "scheduled_run_time", None),
                        duration_ms=stats.last_duration_ms,
                        message=_format_retval(event),
                    )
                )
                self._emit(event.job_id, stats)
                return

            if code & EVENT_JOB_ERROR:
                stats.total_error += 1
                stats.last_event = "error"
                start = self._inflight.pop(event.job_id, stats.last_started_at)
                stats.last_finished_at = now
                stats.last_duration_ms = _calc_duration_ms(start, now)
                stats.last_error = _format_exception(event)
                stats.history.append(
                    JobRunRecord(
                        event="error",
                        recorded_at=now,
                        scheduled_at=getattr(event, "scheduled_run_time", None),
                        duration_ms=stats.last_duration_ms,
                        message=stats.last_error,
                    )
                )
                self._emit(event.job_id, stats)
                return

            if code & EVENT_JOB_MISSED:
                stats.total_missed += 1
                stats.last_event = "missed"
                stats.last_finished_at = now
                stats.last_duration_ms = None
                stats.last_error = _format_missed(event)
                stats.history.append(
                    JobRunRecord(
                        event="missed",
                        recorded_at=now,
                        scheduled_at=getattr(event, "scheduled_run_time", None),
                        message=stats.last_error,
                    )
                )
                self._emit(event.job_id, stats)

    def snapshot(self, job_id: Optional[str] = None) -> Dict[str, Any]:
        """Return serialisable stats for a single job or for all jobs."""
        with self._lock:
            if job_id is not None:
                stats = self._stats.get(job_id)
                return _serialize_stats(stats) if stats else _serialize_stats(self._initial_stats())
            return {job: _serialize_stats(stats) for job, stats in self._stats.items()}

    def _emit(self, job_id: str, stats: JobStats) -> None:
        """Invoke the outbound event callback with the newest job record."""
        if self._on_event is None or not stats.history:
            return
        record = stats.history[-1]
        payload = {
            "type": "event",
            "job_id": job_id,
            "event": record.event,
            "recorded_at": record.recorded_at,
            "scheduled_at": record.scheduled_at,
            "duration_ms": record.duration_ms,
            "message": record.message,
            "stats": _serialize_stats(stats),
        }
        try:
            self._on_event(payload)
        except Exception:
            # Ensure event listener errors do not break scheduler callbacks.
            pass


class SchedulerService:
    """Encapsulates scheduler lifecycle, logging, monitoring, and event streaming."""

    def __init__(
        self,
        *,
        jobs_config: Path | str = "jobs.yaml",
        history_size: int = 50,
        logger_name: str = "scheduler",
    ) -> None:
        """Build the scheduler, logger, and monitor components."""
        self._jobs_config = Path(jobs_config)
        self._logger: Logger = EnhancedLoggerFactory.create_application_logger(
            name=logger_name,
            enable_stdout=True,
            config_prefix="system",
        )
        self._scheduler: AsyncIOScheduler = build_scheduler()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._subscribers: list[asyncio.Queue] = []
        self._subscriber_maxsize = 100
        self._monitor = SchedulerMonitor(
            history_size=history_size,
            on_event=self._handle_monitor_event,
        )
        self._scheduler.add_listener(
            self._monitor.handle_event,
            EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
        )
        self._started = False
        self._started_at: Optional[datetime] = None

    async def startup(self) -> None:
        """Start logging, load configured jobs, and start the scheduler.

        :raises Exception: Propagates APScheduler startup failures.
        """
        if self._started:
            return
        await self._logger.start()
        self._loop = asyncio.get_running_loop()
        load_jobs_from_yaml(self._scheduler, self._jobs_config, etl_logger=self._logger)
        self._scheduler.start()
        self._started = True
        self._started_at = datetime.now(tz=UTC)
        self._logger.info("Scheduler service started")

    async def shutdown(self, *, wait: bool = False) -> None:
        """Stop the scheduler and logger, optionally waiting for jobs to finish.

        :param wait: Whether to wait for running jobs before shutdown completes.
        """
        if not self._started:
            return
        try:
            self._scheduler.shutdown(wait=wait)
        finally:
            self._started = False
            self._logger.info("Scheduler service stopped")
            await self._logger.shutdown()
            self._loop = None

    @property
    def scheduler(self) -> AsyncIOScheduler:
        """Expose the underlying ``AsyncIOScheduler`` instance."""
        return self._scheduler

    @property
    def logger(self) -> Logger:
        """Return the logger that records scheduler lifecycle events."""
        return self._logger

    @property
    def started_at(self) -> Optional[datetime]:
        """Datetime when the scheduler was last started."""
        return self._started_at

    def status(self) -> Dict[str, Any]:
        """Summarise scheduler state, job counts, and timing information."""
        state = self._scheduler.state
        jobs = self._scheduler.get_jobs()
        return {
            "state": _map_state(state),
            "running": state == STATE_RUNNING,
            "job_count": len(jobs),
            "next_run_time": _next_run_time(jobs),
            "started_at": self._started_at,
            "timezone": str(self._scheduler.timezone),
        }

    def list_jobs(self) -> Iterable[Dict[str, Any]]:
        """Yield job definitions enriched with monitoring data."""
        stats_snapshot = self._monitor.snapshot()
        for job in self._scheduler.get_jobs():
            job_stats = stats_snapshot.get(job.id, self._monitor.default_stats())
            yield _serialize_job(job, job_stats)

    def job_details(self, job_id: str) -> Dict[str, Any]:
        """Return a single job payload or raise ``KeyError`` if missing."""
        job = self._scheduler.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        stats = self._monitor.snapshot(job_id)
        return _serialize_job(job, stats)

    async def reload_jobs(self) -> None:
        """Reload job definitions from ``jobs.yaml`` into the running scheduler."""
        load_jobs_from_yaml(self._scheduler, self._jobs_config, etl_logger=self._logger)

    def trigger_job(self, job_id: str, *, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Schedule an immediate run for the job, optionally overriding kwargs.

        :param job_id: Identifier of the job to trigger immediately.
        :param overrides: Optional dictionary of keyword overrides for the run.
        :return: Metadata about the scheduled execution instance.
        :raises KeyError: If the job id is not found.
        """
        job = self._scheduler.get_job(job_id)
        if not job:
            raise KeyError(job_id)

        now = datetime.now(tz=UTC)

        if overrides:
            manual_id = f"{job_id}__manual__{int(now.timestamp() * 1000)}"
            job_kwargs = dict(job.kwargs or {})
            inner_kwargs = dict(job_kwargs.get("kwargs") or {})

            override_payload = dict(overrides)
            if "kwargs" in override_payload:
                explicit_kwargs = override_payload.pop("kwargs") or {}
                inner_kwargs.update(explicit_kwargs)
                scheduler_overrides = override_payload
            else:
                inner_kwargs.update(override_payload)
                scheduler_overrides = {}

            job_kwargs["kwargs"] = inner_kwargs

            for key, value in scheduler_overrides.items():
                job_kwargs[key] = value

            self._scheduler.add_job(
                func=job.func,
                trigger=DateTrigger(run_date=now, timezone=UTC),
                args=job.args,
                kwargs=job_kwargs,
                id=manual_id,
                replace_existing=False,
            )
            scheduled_job_id = manual_id
        else:
            job.modify(next_run_time=now)
            scheduled_job_id = job.id

        self._scheduler.wakeup()
        self._logger.info(f"Manual trigger requested for job {job_id} (override={bool(overrides)})")
        return {
            "job_id": job_id,
            "scheduled_job_id": scheduled_job_id,
            "scheduled_for": now,
            "overrides": overrides or {},
        }

    def pause_job(self, job_id: str) -> Dict[str, Any]:
        """Temporarily pause a job so it no longer runs on its schedule.

        :param job_id: Identifier of the job to pause.
        :return: Updated job metadata after pausing.
        :raises KeyError: If the job id is not found.
        """

        job = self._scheduler.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        self._scheduler.pause_job(job_id)
        self._logger.info(f"Paused job {job_id}")
        return self.job_details(job_id)

    def resume_job(self, job_id: str) -> Dict[str, Any]:
        """Resume a previously paused job.

        :param job_id: Identifier of the job to resume.
        :return: Updated job metadata after resuming.
        :raises KeyError: If the job id is not found.
        """

        job = self._scheduler.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        self._scheduler.resume_job(job_id)
        self._logger.info(f"Resumed job {job_id}")
        return self.job_details(job_id)
