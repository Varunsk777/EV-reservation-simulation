from __future__ import annotations

from services.orchestration_service import orchestration_service


def handle_manual_event(payload) -> dict[str, object]:
    if payload.event_type.upper() == "RESET":
        orchestration_service.reset()
        return {"status": "CONFIRMED", "message": "Simulation reset"}
    return {
        "status": "REJECTED_CONFLICT",
        "message": "Manual point events were replaced by orchestration controls.",
    }
