from __future__ import annotations

from services.orchestration_service import orchestration_service


SLOT_TIMES = ["dynamic"]


def load_stations_into_redis() -> None:
    orchestration_service.rebuild_redis_state()


def reset_station_cache() -> None:
    orchestration_service.rebuild_redis_state()


def snapshot_stations() -> list[dict[str, object]]:
    return orchestration_service.snapshot()["stations"]


def toggle_slot(*, station_id: int, point_id: int, slot_time: str) -> str:
    return "reserved"


def mark_slot_status(*, station_id: int, point_id: int, slot_time: str, status: str) -> None:
    return None
