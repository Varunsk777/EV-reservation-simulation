from __future__ import annotations

import random
from datetime import datetime, timedelta

from faker import Faker


fake = Faker()

AVAILABLE_STATIONS = [1, 2, 3]
AVAILABLE_VEHICLE_IDS = list(range(1, 101))
DURATIONS_MINUTES = [30, 45, 60]


def _format_datetime(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat(sep=" ")


def generate_vehicle():
    return {
        "vehicle_id": random.choice(AVAILABLE_VEHICLE_IDS),
        "priority": random.choice([1, 2, 3]),
        "battery_needed": random.randint(10, 60),
        "arrival_time": fake.time(),
    }


def generate_reservation_request(vehicle: dict, base_time: datetime | None = None) -> dict:
    current_time = base_time or datetime.now()
    start_time = current_time + timedelta(minutes=random.randint(0, 60))
    duration_minutes = random.choice(DURATIONS_MINUTES)
    end_time = start_time + timedelta(minutes=duration_minutes)

    return {
        "vehicle": vehicle,
        "station_id": random.choice(AVAILABLE_STATIONS),
        "start_time": _format_datetime(start_time),
        "end_time": _format_datetime(end_time),
        "duration_minutes": duration_minutes,
    }


def build_retry_request(request: dict) -> dict:
    current_station = request["station_id"]
    alternative_stations = [
        station_id for station_id in AVAILABLE_STATIONS if station_id != current_station
    ]

    retry_request = dict(request)

    if alternative_stations and random.choice([True, False]):
        retry_request["station_id"] = random.choice(alternative_stations)
    else:
        shifted_start = datetime.fromisoformat(request["start_time"]) + timedelta(minutes=10)
        shifted_end = datetime.fromisoformat(request["end_time"]) + timedelta(minutes=10)
        retry_request["start_time"] = _format_datetime(shifted_start)
        retry_request["end_time"] = _format_datetime(shifted_end)

    return retry_request


def get_request_delay(step_index: int, total_iterations: int) -> float:
    midpoint = max(total_iterations // 2, 1)
    if step_index < midpoint:
        return random.uniform(2.0, 4.0)
    return random.uniform(0.5, 2.0)
