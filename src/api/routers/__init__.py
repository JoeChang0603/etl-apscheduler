"""HTTP router factory wiring health, scheduler, and system endpoints."""

from fastapi import APIRouter

from . import health, scheduler, system


def create_router() -> APIRouter:
    router = APIRouter()
    router.include_router(health.router, prefix="/health")
    router.include_router(scheduler.router, prefix="/scheduler")
    router.include_router(system.router, prefix="/system")
    return router
