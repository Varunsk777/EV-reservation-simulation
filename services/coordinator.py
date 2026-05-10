from __future__ import annotations

from services.orchestration_service import orchestration_service


def choose_best_station(station_ids):
    decision = orchestration_service.snapshot()["decision"]
    selected = decision.get("selected_station")
    return selected if selected in station_ids else None


def controlled_test_reservation() -> dict[str, object]:
    orchestration_service.start()
    return {"success": True, "message": "Orchestration simulation started", "slot_id": None}
