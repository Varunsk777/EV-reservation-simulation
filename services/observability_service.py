from __future__ import annotations

from services.orchestration_service import orchestration_service


def get_activity(limit: int = 10) -> dict[str, object]:
    events = orchestration_service.snapshot()["events"][:limit]
    return {"reservations": [], "conflicts": [], "reroutes": [], "events": events}


def get_metrics() -> dict[str, object]:
    metrics = orchestration_service.snapshot()["metrics"]
    return {
        "total_requests": metrics.get("active_vehicles", 0),
        "success": metrics.get("active_sessions", 0),
        "failed": 0,
        "avg_wait_time": 0,
        "utilization": 0,
        "system_load": 0,
        **metrics,
    }


def get_redis_state() -> dict[str, object]:
    payload = orchestration_service.snapshot()
    return {
        "connected": True,
        "station_states": payload["stations"],
        "active_sessions": payload["metrics"].get("active_sessions", 0),
        "locks": [],
    }
