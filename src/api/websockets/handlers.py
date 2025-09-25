"""WebSocket handlers streaming scheduler snapshots and live events."""

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder

from api.dependencies import get_scheduler_service


def register(app: FastAPI) -> None:
    @app.websocket("/ws/scheduler")
    async def scheduler_events(websocket: WebSocket) -> None:
        await websocket.accept()

        try:
            service = get_scheduler_service()
        except HTTPException as exc:  # pragma: no cover - defensive guard
            await websocket.send_json({"type": "error", "detail": exc.detail})
            await websocket.close(code=1011)
            return

        queue = service.subscribe()
        try:
            snapshot = {
                "type": "snapshot",
                "status": service.status(),
                "jobs": list(service.list_jobs()),
            }
            await websocket.send_json(jsonable_encoder(snapshot))

            while True:
                event = await queue.get()
                await websocket.send_json(jsonable_encoder(event))
        except WebSocketDisconnect:
            pass
        finally:
            service.unsubscribe(queue)
