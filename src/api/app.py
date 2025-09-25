"""FastAPI application entrypoint that boots the scheduler service."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.dependencies import set_scheduler_service
from api.routers import create_router
from api.websockets import register_websockets
from scheduler.service import SchedulerService


service = SchedulerService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    set_scheduler_service(service)
    await service.startup()
    try:
        yield
    finally:
        try:
            await service.shutdown()
        finally:
            set_scheduler_service(None)


def create_app() -> FastAPI:
    app = FastAPI(title="Kairos ETL System Scheduler API", version="1.0.0", lifespan=lifespan)
    app.include_router(create_router())
    register_websockets(app)
    return app


app = create_app()
