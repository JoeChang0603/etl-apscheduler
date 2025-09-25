"""System metrics endpoint exposing container resource statistics."""

from fastapi import APIRouter

from utils.system_metrics import collect_metrics


router = APIRouter()


@router.get("/metrics", tags=["system"])
async def system_metrics() -> dict:
    return collect_metrics()
