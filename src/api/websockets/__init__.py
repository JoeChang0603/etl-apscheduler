"""WebSocket registration helpers for scheduler event streaming."""

from fastapi import FastAPI

from .handlers import register


def register_websockets(app: FastAPI) -> None:
    register(app)
