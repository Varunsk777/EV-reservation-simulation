from __future__ import annotations

import logging
import threading

from fastapi import APIRouter, status

from models.schemas import (
    ReservationRequest,
    ReservationResponse,
    SimulationStartRequest,
    SimulationStartResponse,
)
from services.coordinator import reserve_slot
from services.dashboard_service import dashboard_state
from services.station_service import load_stations_into_redis, reset_station_cache
from simulation.simulator import run_simulation


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["dashboard"])


@router.post(
    "/reserve",
    response_model=ReservationResponse,
    status_code=status.HTTP_200_OK,
)
def create_reservation_route(payload: ReservationRequest) -> ReservationResponse:
    result = reserve_slot(
        vehicle_id=payload.vehicle_id,
        station_id=payload.station_id,
        start_time=payload.start_time,
        end_time=payload.end_time,
    )
    return ReservationResponse(**result)


@router.get("/dashboard")
def get_dashboard() -> dict[str, object]:
    return dashboard_state.snapshot()


def _run_simulation_job(iterations: int) -> None:
    try:
        dashboard_state.reset_run()
        reset_station_cache()
        load_stations_into_redis()
        run_simulation(iterations=iterations, dashboard_state=dashboard_state)
    finally:
        dashboard_state.set_running(False)


@router.post(
    "/simulation/start",
    response_model=SimulationStartResponse,
    status_code=status.HTTP_200_OK,
)
def start_simulation(payload: SimulationStartRequest) -> SimulationStartResponse:
    if dashboard_state.is_running():
        return SimulationStartResponse(
            started=False,
            message="Simulation is already running.",
        )

    dashboard_state.set_running(True)
    worker = threading.Thread(
        target=_run_simulation_job,
        args=(payload.iterations,),
        daemon=True,
    )
    worker.start()

    return SimulationStartResponse(
        started=True,
        message=f"Simulation started with {payload.iterations} iterations.",
    )
