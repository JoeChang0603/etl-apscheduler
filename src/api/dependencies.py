"""Shared FastAPI dependencies exposing the scheduler service singleton."""

from __future__ import annotations

from typing import Optional

from fastapi import Depends, HTTPException

from scheduler.service import SchedulerService


_scheduler_service: Optional[SchedulerService] = None


def set_scheduler_service(service: Optional[SchedulerService]) -> None:
    global _scheduler_service
    _scheduler_service = service


def get_scheduler_service() -> SchedulerService:
    if _scheduler_service is None:
        raise HTTPException(status_code=503, detail="Scheduler service not ready")
    return _scheduler_service


def scheduler_service_dependency(service: SchedulerService = Depends(get_scheduler_service)) -> SchedulerService:
    return service
