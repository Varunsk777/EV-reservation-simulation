from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from services.orchestration_service import orchestration_service


class SimulationControlRequest(BaseModel):
    speed: int | None = Field(default=None, ge=1, le=20)


api_router = APIRouter(prefix="/api", tags=["orchestration"])
router = APIRouter(tags=["compatibility"])


@api_router.get("/dashboard")
def dashboard() -> dict[str, object]:
    return orchestration_service.snapshot()


@api_router.post("/simulation/start")
def start_simulation(payload: SimulationControlRequest) -> dict[str, object]:
    started = orchestration_service.start(speed=payload.speed)
    return {
        "started": started,
        "running": orchestration_service.is_running(),
        "message": "Simulation running" if started else "Simulation resumed",
    }


@api_router.post("/simulation/pause")
def pause_simulation() -> dict[str, object]:
    orchestration_service.pause()
    return {"paused": True}


@api_router.post("/simulation/resume")
def resume_simulation() -> dict[str, object]:
    orchestration_service.resume()
    return {"running": True}


@api_router.post("/simulation/speed")
def set_simulation_speed(payload: SimulationControlRequest) -> dict[str, object]:
    if payload.speed is not None:
        orchestration_service.set_speed(payload.speed)
    return {"clock": orchestration_service.snapshot()["clock"]}


@api_router.post("/simulation/reset")
def reset_simulation() -> dict[str, object]:
    orchestration_service.reset()
    return {"reset": True, "payload": orchestration_service.snapshot()}


@api_router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    await orchestration_service.register_websocket(websocket)
    try:
        while True:
            message = await websocket.receive_json()
            action = message.get("action")
            if action == "start":
                orchestration_service.start(speed=message.get("speed"))
            elif action == "pause":
                orchestration_service.pause()
            elif action == "resume":
                orchestration_service.resume()
            elif action == "reset":
                orchestration_service.reset()
            elif action == "speed":
                orchestration_service.set_speed(int(message.get("speed", 2)))
    except WebSocketDisconnect:
        orchestration_service.unregister_websocket(websocket)
