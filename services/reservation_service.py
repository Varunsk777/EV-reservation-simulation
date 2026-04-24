from __future__ import annotations

from datetime import datetime

from services.coordinator import reserve_slot


def create_reservation(vehicle_id, station_id, start_time, end_time):
    if isinstance(start_time, str):
        start_time = datetime.fromisoformat(start_time)
    if isinstance(end_time, str):
        end_time = datetime.fromisoformat(end_time)

    result = reserve_slot(
        vehicle_id=vehicle_id,
        station_id=station_id,
        start_time=start_time,
        end_time=end_time,
    )
    return result.get("slot_id")


def process_queue(station_id):
    print(f"Queue processing is not implemented for station {station_id}.")


def release_slot(station_id, slot_id):
    print(f"Slot {slot_id} released for station {station_id}.")
