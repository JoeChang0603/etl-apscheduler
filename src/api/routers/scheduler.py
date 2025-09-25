"""Scheduler management endpoints for querying and controlling jobs."""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict

from api.dependencies import scheduler_service_dependency
from scheduler.service import SchedulerService


router = APIRouter()


class TriggerJobRequest(BaseModel):
    overrides: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "overrides": {
                    "kwargs": {
                        "mode": "merge",
                        "db_name": "C_DATA",
                        "coll_name": "portfolio",
                        "documents": [
                            {
                                "portfolio": "demo",
                                "status": "active",
                                "updated_at": "2024-09-30T00:00:00Z"
                            }
                        ],
                        "key_fields": ["portfolio"],
                        "merge_strategy": "set"
                    }
                }
            }
        }
    )


@router.get("/status", tags=["scheduler"])
async def scheduler_status(service: SchedulerService = Depends(scheduler_service_dependency)) -> dict:
    return service.status()


@router.get("/jobs", tags=["scheduler"])
async def list_jobs(service: SchedulerService = Depends(scheduler_service_dependency)) -> dict:
    jobs = list(service.list_jobs())
    return {"jobs": jobs, "count": len(jobs)}


@router.get("/jobs/{job_id}", tags=["scheduler"])
async def job_details(job_id: str, service: SchedulerService = Depends(scheduler_service_dependency)) -> dict:
    try:
        return service.job_details(job_id)
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found") from exc


@router.post("/reload", tags=["scheduler"], status_code=status.HTTP_202_ACCEPTED)
async def reload_jobs(service: SchedulerService = Depends(scheduler_service_dependency)) -> dict:
    await service.reload_jobs()
    return {"message": "Jobs reloaded"}


@router.post("/jobs/{job_id}/trigger", tags=["scheduler"], status_code=status.HTTP_202_ACCEPTED)
async def trigger_job(
    job_id: str,
    payload: Optional[TriggerJobRequest] = None,
    service: SchedulerService = Depends(scheduler_service_dependency),
) -> dict:
    try:
        result = service.trigger_job(job_id, overrides=payload.overrides if payload else None)
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found") from exc
    return result


@router.post("/jobs/{job_id}/pause", tags=["scheduler"], status_code=status.HTTP_202_ACCEPTED)
async def pause_job(job_id: str, service: SchedulerService = Depends(scheduler_service_dependency)) -> dict:
    try:
        return service.pause_job(job_id)
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found") from exc


@router.post("/jobs/{job_id}/resume", tags=["scheduler"], status_code=status.HTTP_202_ACCEPTED)
async def resume_job(job_id: str, service: SchedulerService = Depends(scheduler_service_dependency)) -> dict:
    try:
        return service.resume_job(job_id)
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found") from exc
