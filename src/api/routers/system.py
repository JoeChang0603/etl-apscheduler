"""System metrics endpoint exposing container resource statistics."""

from fastapi import APIRouter

from utils.system_metrics import collect_metrics, collect_resource_snapshot


router = APIRouter()


@router.get("/metrics", tags=["system"])
async def system_metrics() -> dict:
    return collect_metrics()


@router.get("/resources", tags=["system"])
async def system_resources(device: str = "/dev/nvme0n1p1") -> dict:
    return collect_resource_snapshot(device)
