from __future__ import annotations

import time

from services.orchestration_service import orchestration_service


def run_simulation(iterations=5, dashboard_state=None):
    orchestration_service.start()
    for _ in range(max(1, int(iterations))):
        time.sleep(1)
    orchestration_service.pause()
    if dashboard_state is not None:
        dashboard_state.add_log("Orchestration simulation cycle completed.")
