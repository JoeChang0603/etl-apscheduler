from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, Optional

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
        self._lock = Lock()
        self._stats: Dict[str, JobStats] = {}
        self._inflight: Dict[str, datetime] = {}
        self._history_size = history_size
        self._on_event = on_event

    def _initial_stats(self) -> JobStats:
        stats = JobStats()
        stats.history = deque(maxlen=self._history_size)
        return stats

    def default_stats(self) -> Dict[str, Any]:
        return _serialize_stats(self._initial_stats())

    def handle_event(self, event: JobEvent) -> None:
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
        with self._lock:
            if job_id is not None:
                stats = self._stats.get(job_id)
                return _serialize_stats(stats) if stats else _serialize_stats(self._initial_stats())
            return {job: _serialize_stats(stats) for job, stats in self._stats.items()}

    def _emit(self, job_id: str, stats: JobStats) -> None:
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
    """Encapsulates scheduler lifecycle, logging, and monitoring state."""

    def __init__(
        self,
        *,
        jobs_config: Path | str = "jobs.yaml",
        history_size: int = 50,
        logger_name: str = "scheduler",
    ) -> None:
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
        return self._scheduler

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def started_at(self) -> Optional[datetime]:
        return self._started_at

    def status(self) -> Dict[str, Any]:
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
        stats_snapshot = self._monitor.snapshot()
        for job in self._scheduler.get_jobs():
            job_stats = stats_snapshot.get(job.id, self._monitor.default_stats())
            yield _serialize_job(job, job_stats)

    def job_details(self, job_id: str) -> Dict[str, Any]:
        job = self._scheduler.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        stats = self._monitor.snapshot(job_id)
        return _serialize_job(job, stats)

    async def reload_jobs(self) -> None:
        load_jobs_from_yaml(self._scheduler, self._jobs_config, etl_logger=self._logger)

    def trigger_job(self, job_id: str, *, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        job = self._scheduler.get_job(job_id)
        if not job:
            raise KeyError(job_id)

        now = datetime.now(tz=UTC)

        if overrides:
            manual_id = f"{job_id}__manual__{int(now.timestamp() * 1000)}"
            job_kwargs = dict(job.kwargs or {})
            inner_kwargs = dict(job_kwargs.get("kwargs") or {})
            inner_kwargs.update(overrides)
            job_kwargs["kwargs"] = inner_kwargs

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
        self._logger.info("Manual trigger requested for job %s (override=%s)", job_id, bool(overrides))
        return {
            "job_id": job_id,
            "scheduled_job_id": scheduled_job_id,
            "scheduled_for": now,
            "overrides": overrides or {},
        }

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._subscriber_maxsize)
        self._subscribers.append(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        try:
            self._subscribers.remove(queue)
        except ValueError:
            pass

    def _handle_monitor_event(self, payload: Dict[str, Any]) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._broadcast_event, payload)

    def _broadcast_event(self, payload: Dict[str, Any]) -> None:
        stale: list[asyncio.Queue] = []
        for queue in self._subscribers:
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    queue.put_nowait(payload)
                except asyncio.QueueFull:
                    stale.append(queue)
        for queue in stale:
            try:
                self._subscribers.remove(queue)
            except ValueError:
                continue


def _calc_duration_ms(start: Optional[datetime], end: datetime) -> Optional[float]:
    if not start:
        return None
    return (end - start).total_seconds() * 1000


def _format_exception(event: JobExecutionEvent) -> str:
    exc = getattr(event, "exception", None)
    if exc is None:
        return "Unknown error"
    return f"{type(exc).__name__}: {exc}"


def _format_retval(event: JobExecutionEvent) -> Optional[str]:
    retval = getattr(event, "retval", None)
    if retval is None:
        return None
    return str(retval)


def _format_missed(event: JobEvent) -> str:
    scheduled = getattr(event, "scheduled_run_time", None)
    if scheduled is None:
        return "Job missed its scheduled run"
    return f"Job missed run scheduled for {scheduled.isoformat()}"


def _serialize_stats(stats: JobStats) -> Dict[str, Any]:
    history = [
        {
            "event": record.event,
            "recorded_at": record.recorded_at,
            "scheduled_at": record.scheduled_at,
            "duration_ms": record.duration_ms,
            "message": record.message,
        }
        for record in list(stats.history)
    ]
    return {
        "total_runs": stats.total_runs,
        "total_success": stats.total_success,
        "total_error": stats.total_error,
        "total_missed": stats.total_missed,
        "last_event": stats.last_event,
        "last_scheduled_at": stats.last_scheduled_at,
        "last_started_at": stats.last_started_at,
        "last_finished_at": stats.last_finished_at,
        "last_duration_ms": stats.last_duration_ms,
        "last_error": stats.last_error,
        "history": history,
    }


def _serialize_job(job: Any, stats: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": job.id,
        "name": job.name,
        "func_ref": job.func_ref,
        "next_run_time": job.next_run_time,
        "trigger": str(job.trigger),
        "kwargs": job.kwargs,
        "coalesce": job.coalesce,
        "max_instances": job.max_instances,
        "misfire_grace_time": job.misfire_grace_time,
        "stats": stats,
    }


def _map_state(state: int) -> str:
    if state == STATE_RUNNING:
        return "running"
    if state == STATE_PAUSED:
        return "paused"
    if state == STATE_STOPPED:
        return "stopped"
    return f"unknown({state})"


def _next_run_time(jobs: Iterable[Any]) -> Optional[datetime]:
    next_times = [job.next_run_time for job in jobs if job.next_run_time is not None]
    if not next_times:
        return None
    return min(next_times)
