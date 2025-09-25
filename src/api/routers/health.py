"""Health-check endpoint returning the current scheduler status."""

from fastapi import APIRouter

from api.dependencies import get_scheduler_service


router = APIRouter()


@router.get("/", tags=["health"])
async def healthcheck() -> dict:
    service = get_scheduler_service()
    status = service.status()
    return {
        "ok": True,
        "scheduler": status,
    }
