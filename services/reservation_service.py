from __future__ import annotations

from datetime import datetime

from core.database import db_connection
from repositories.reservation_repo import create_reservation as insert_reservation
from repositories.reservation_repo import list_reservations, reservation_conflict_exists


class ReservationServiceError(Exception):
    pass


class NotFoundError(ReservationServiceError):
    pass


class ConnectorMismatchError(ReservationServiceError):
    pass


def reserve_interval(
    *,
    user_id: int | None,
    station_id: int,
    charger_id: int,
    start_time: datetime,
    end_time: datetime,
) -> dict[str, object]:
    if start_time >= end_time:
        raise ValueError("start_time must be before end_time")
    with db_connection() as conn:
        if reservation_conflict_exists(conn, station_id, charger_id, start_time, end_time):
            return {"status": "REJECTED_CONFLICT"}
        reservation_id = insert_reservation(
            conn,
            user_id=user_id,
            station_id=station_id,
            charger_id=charger_id,
            start=start_time,
            end=end_time,
        )
        conn.commit()
        return {"status": "CONFIRMED", "reservation_id": reservation_id}


def list_slot_reservations() -> list[dict[str, object]]:
    with db_connection() as conn:
        return list_reservations(conn)


def recommend_charging_points(**kwargs) -> dict[str, object]:
    return {"status": "USE_ORCHESTRATION_DASHBOARD"}


def reserve_charging_point(**kwargs) -> dict[str, object]:
    raise ReservationServiceError("point-based reservations were replaced by charger interval orchestration")


def occupy_time_slot(**kwargs) -> dict[str, object]:
    raise ReservationServiceError("fixed slot occupation was replaced by dynamic interval allocation")
