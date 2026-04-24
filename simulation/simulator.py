from __future__ import annotations

import time

from simulation.agents.vehicle_agent import vehicle_request
from simulation.generator import (
    build_retry_request,
    generate_reservation_request,
    generate_vehicle,
    get_request_delay,
)


def run_simulation(iterations=5, dashboard_state=None):
    print("\nStarting Multi-Agent Simulation...\n")

    if dashboard_state is not None:
        dashboard_state.add_log(f"Simulation started with {iterations} iterations.")

    for i in range(iterations):
        print(f"\nStep {i + 1}")

        vehicle = generate_vehicle()
        request = generate_reservation_request(vehicle)
        result = vehicle_request(request, dashboard_state=dashboard_state)

        if not result["success"]:
            retry_request = build_retry_request(request)
            print(
                "Retrying once with adjusted request:"
                f" vehicle_id={retry_request['vehicle']['vehicle_id']}"
                f" station_id={retry_request['station_id']}"
                f" start_time={retry_request['start_time']}"
                f" end_time={retry_request['end_time']}"
            )
            if dashboard_state is not None:
                dashboard_state.add_log(
                    f"Retrying vehicle {retry_request['vehicle']['vehicle_id']} at station {retry_request['station_id']}."
                )
            vehicle_request(retry_request, dashboard_state=dashboard_state)

        delay_seconds = get_request_delay(i, iterations)
        print(f"Waiting {delay_seconds:.2f}s before next request")
        time.sleep(delay_seconds)

    print("\nSimulation finished")
    if dashboard_state is not None:
        dashboard_state.add_log("Simulation finished.")
