from __future__ import annotations

from services.reservation_service import create_reservation
from services.vehicle_service import register_vehicle


def vehicle_request(request: dict, dashboard_state=None):
    vehicle = request["vehicle"]

    print(
        "Request:"
        f" vehicle_id={vehicle['vehicle_id']}"
        f" station_id={request['station_id']}"
        f" start_time={request['start_time']}"
        f" end_time={request['end_time']}"
    )

    db_vehicle_id = register_vehicle(vehicle)
    if not db_vehicle_id:
        print(
            "Result:"
            f" vehicle_id={vehicle['vehicle_id']}"
            " status=failure"
            " reason=vehicle_registration_failed"
        )
        result = {
            "success": False,
            "message": "Vehicle registration failed",
            "slot_id": None,
        }
        if dashboard_state is not None:
            dashboard_state.record_request(
                vehicle_id=vehicle["vehicle_id"],
                station_id=request["station_id"],
                start_time=request["start_time"],
                end_time=request["end_time"],
                success=False,
                message=result["message"],
                slot_id=None,
            )
            dashboard_state.add_log(
                f"Vehicle {vehicle['vehicle_id']} failed registration for station {request['station_id']}."
            )
        return result

    slot_id = create_reservation(
        vehicle_id=db_vehicle_id,
        station_id=request["station_id"],
        start_time=request["start_time"],
        end_time=request["end_time"],
    )

    success = slot_id is not None
    result_label = "success" if success else "failure"
    result = {
        "success": success,
        "message": "Reservation confirmed" if success else "Reservation failed",
        "slot_id": slot_id,
    }

    print(
        "Result:"
        f" vehicle_id={vehicle['vehicle_id']}"
        f" station_id={request['station_id']}"
        f" start_time={request['start_time']}"
        f" end_time={request['end_time']}"
        f" status={result_label}"
        f" slot_id={slot_id}"
    )

    if dashboard_state is not None:
        dashboard_state.record_request(
            vehicle_id=vehicle["vehicle_id"],
            station_id=request["station_id"],
            start_time=request["start_time"],
            end_time=request["end_time"],
            success=success,
            message=result["message"],
            slot_id=slot_id,
        )
        dashboard_state.add_log(
            f"Vehicle {vehicle['vehicle_id']} -> Station {request['station_id']} -> {result['message']}"
        )

    return result
