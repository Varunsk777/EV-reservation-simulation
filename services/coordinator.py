from __future__ import annotations

from datetime import datetime, timedelta

from psycopg2 import Error as PsycopgError
from psycopg2 import extensions
from redis.exceptions import RedisError

from core.database import get_db_connection
from core.redis import redis_client
from repositories.reservation_repo import (
    check_conflict,
    create_reservation,
    get_station_slots,
)


LOCK_TTL_SECONDS = 300
DEBUG_MODE = True
DEBUG_DISABLE_REDIS_LOCK = True


def choose_best_station(station_ids):
    best_station = None
    best_score = float("inf")

    for station_id in station_ids:
        slots = redis_client.hgetall(f"station:{station_id}:slots")
        available = sum(1 for status in slots.values() if status == "available")
        queue_length = redis_client.llen(f"station:{station_id}:queue")
        score = queue_length - available

        print(
            f"[DEBUG] station_id={station_id} available={available} "
            f"queue_length={queue_length} score={score}"
        )

        if score < best_score:
            best_score = score
            best_station = station_id

    return best_station


def _coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def reserve_slot(
    vehicle_id: int,
    station_id: int,
    start_time: datetime | str,
    end_time: datetime | str,
) -> dict[str, object]:
    start_dt = _coerce_datetime(start_time)
    end_dt = _coerce_datetime(end_time)

    print(
        f"[DEBUG] reservation attempt vehicle_id={vehicle_id} "
        f"station_id={station_id} start_time={start_dt.isoformat()} "
        f"end_time={end_dt.isoformat()}"
    )

    if start_dt >= end_dt:
        print("[DEBUG] rejected: start_time is not before end_time")
        return {
            "success": False,
            "message": "DB error",
            "slot_id": None,
        }

    try:
        fetch_conn = get_db_connection()
    except PsycopgError as exc:
        print(f"[DEBUG] DB connection error while fetching slots: {exc}")
        return {
            "success": False,
            "message": "DB error",
            "slot_id": None,
        }

    try:
        slot_ids = get_station_slots(fetch_conn, station_id)
    except PsycopgError as exc:
        print(f"[DEBUG] slot fetch failed: {exc}")
        return {
            "success": False,
            "message": "DB error",
            "slot_id": None,
        }
    finally:
        fetch_conn.close()

    if not slot_ids:
        print(f"[DEBUG] no slots found for station {station_id}")
        return {
            "success": False,
            "message": "No slots found for station",
            "slot_id": None,
        }

    unix_timestamp = int(start_dt.timestamp())
    print(f"[DEBUG] unix timestamp for Redis key: {unix_timestamp}")

    saw_conflict = False
    saw_redis_failure = False
    saw_db_error = False

    for slot_id in slot_ids:
        print(
            f"[DEBUG] checking slot_id={slot_id} "
            f"start_time={start_dt.isoformat()} end_time={end_dt.isoformat()}"
        )

        try:
            check_conn = get_db_connection()
        except PsycopgError as exc:
            saw_db_error = True
            print(f"[DEBUG] failed to open DB connection for slot {slot_id}: {exc}")
            print(f"[DEBUG] slot {slot_id} rejected due to DB error")
            continue

        try:
            conflict_exists = check_conflict(check_conn, slot_id, start_dt, end_dt)
            print(f"[DEBUG] conflict decision for slot {slot_id}: {conflict_exists}")
        except PsycopgError as exc:
            saw_db_error = True
            print(f"[DEBUG] conflict query failed for slot {slot_id}: {exc}")
            print(f"[DEBUG] slot {slot_id} rejected due to DB error")
            continue
        finally:
            check_conn.close()

        if conflict_exists:
            saw_conflict = True
            print(f"[DEBUG] slot {slot_id} rejected due to conflict")
            continue

        lock_key = f"slot:{slot_id}:{unix_timestamp}"
        print(f"[DEBUG] Redis key used: {lock_key}")

        if DEBUG_MODE and DEBUG_DISABLE_REDIS_LOCK:
            lock_acquired = True
            print(f"[DEBUG] Redis lock bypass enabled for slot {slot_id}")
        else:
            try:
                lock_acquired = bool(
                    redis_client.set(
                        lock_key,
                        f"{vehicle_id}:{station_id}:{unix_timestamp}",
                        nx=True,
                        ex=LOCK_TTL_SECONDS,
                    )
                )
                print(f"[DEBUG] Redis lock success for slot {slot_id}: {lock_acquired}")
            except RedisError as exc:
                saw_redis_failure = True
                print(f"[DEBUG] Redis failure for slot {slot_id}: {exc}")
                print(f"[DEBUG] slot {slot_id} rejected due to Redis failure")
                continue

        if not lock_acquired:
            saw_redis_failure = True
            print(f"[DEBUG] Redis lock failure for slot {slot_id}")
            print(f"[DEBUG] slot {slot_id} rejected due to Redis failure")
            continue

        reservation_conn = None

        try:
            reservation_conn = get_db_connection()
            reservation_conn.set_session(autocommit=False)
            reservation_conn.set_isolation_level(extensions.ISOLATION_LEVEL_READ_COMMITTED)

            second_conflict = check_conflict(reservation_conn, slot_id, start_dt, end_dt)
            print(f"[DEBUG] second conflict decision for slot {slot_id}: {second_conflict}")

            if second_conflict:
                saw_conflict = True
                reservation_conn.rollback()
                print(f"[DEBUG] slot {slot_id} rejected after second conflict check")
                continue

            reservation_id = create_reservation(
                reservation_conn,
                vehicle_id=vehicle_id,
                station_id=station_id,
                slot_id=slot_id,
                start=start_dt,
                end=end_dt,
            )
            reservation_conn.commit()

            print(
                f"[DEBUG] slot {slot_id} accepted, reservation_id={reservation_id}"
            )
            return {
                "success": True,
                "message": "Reservation confirmed",
                "slot_id": slot_id,
            }
        except PsycopgError as exc:
            saw_db_error = True
            if reservation_conn is not None:
                reservation_conn.rollback()
            print(f"[DEBUG] insert failed for slot {slot_id}: {exc}")
            print(f"[DEBUG] slot {slot_id} rejected due to DB error")
        finally:
            if reservation_conn is not None:
                reservation_conn.close()
            if not (DEBUG_MODE and DEBUG_DISABLE_REDIS_LOCK):
                try:
                    redis_client.delete(lock_key)
                except RedisError as exc:
                    saw_redis_failure = True
                    print(f"[DEBUG] Redis unlock failed for slot {slot_id}: {exc}")

    if saw_conflict:
        failure_message = "Conflict detected"
    elif saw_db_error:
        failure_message = "DB error"
    elif saw_redis_failure:
        failure_message = "Redis failure"
    else:
        failure_message = "No slots available"

    print(f"[DEBUG] final reservation result: {failure_message}")
    return {
        "success": False,
        "message": failure_message,
        "slot_id": None,
    }


def controlled_test_reservation() -> dict[str, object]:
    start_time = datetime.now() + timedelta(minutes=10)
    end_time = start_time + timedelta(minutes=30)

    print(
        f"[DEBUG] controlled test station_id=1 "
        f"start_time={start_time.isoformat()} end_time={end_time.isoformat()}"
    )

    return reserve_slot(
        vehicle_id=1,
        station_id=1,
        start_time=start_time,
        end_time=end_time,
    )
