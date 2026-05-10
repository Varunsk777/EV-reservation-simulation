from __future__ import annotations

from datetime import datetime, timezone

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import RealDictCursor


ACTIVE_STATUSES = ("reserved", "waiting", "charging")


def list_reservations(conn: PgConnection) -> list[dict[str, object]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COALESCE(r.id, r.reservation_id) AS id,
                r.user_id,
                r.station_id,
                r.charger_id,
                r.reservation_start,
                r.reservation_end,
                r.reservation_status,
                r.created_at,
                u.vehicle_id
            FROM core.reservations r
            LEFT JOIN core.users u ON u.user_id = r.user_id
            ORDER BY r.reservation_start ASC, r.reservation_id ASC;
            """
        )
        return [dict(row) for row in cur.fetchall()]


def reservation_conflict_exists(
    conn: PgConnection,
    station_id: int,
    charger_id: int,
    start: datetime,
    end: datetime,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM core.reservations
                WHERE station_id = %s
                  AND charger_id = %s
                  AND reservation_status = ANY(%s)
                  AND reservation_start < %s
                  AND reservation_end > %s
            );
            """,
            (station_id, charger_id, list(ACTIVE_STATUSES), end, start),
        )
        return bool(cur.fetchone()[0])


def create_reservation(
    conn: PgConnection,
    *,
    user_id: int | None,
    station_id: int,
    charger_id: int,
    start: datetime,
    end: datetime,
    status: str = "reserved",
) -> int:
    slot_id = station_id * 100 + charger_id

    def naive_utc(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    sched_start = naive_utc(start)
    sched_end = naive_utc(end)
    expires_at = sched_end
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO core.reservations (
                slot_id,
                station_id,
                status,
                reserved_at,
                expires_at,
                scheduled_start,
                scheduled_end,
                point_id,
                booking_status,
                user_id,
                charger_id,
                reservation_start,
                reservation_end,
                reservation_status,
                created_at,
                vehicle_id
            )
            VALUES (
                %s, %s, 'confirmed',
                CURRENT_TIMESTAMP, %s,
                %s, %s,
                %s, 'CONFIRMED',
                %s, %s,
                %s, %s,
                %s, CURRENT_TIMESTAMP,
                NULL
            )
            RETURNING reservation_id;
            """,
            (
                slot_id,
                station_id,
                expires_at,
                sched_start,
                sched_end,
                charger_id,
                user_id,
                charger_id,
                start,
                end,
                status,
            ),
        )
        rid = int(cur.fetchone()[0])
        cur.execute(
            "UPDATE core.reservations SET id = reservation_id WHERE reservation_id = %s;",
            (rid,),
        )
        return rid


def get_station_chargers(conn: PgConnection, station_id: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(charger_count, total_slots, 1) FROM core.stations WHERE station_id = %s;",
            (station_id,),
        )
        row = cur.fetchone()
    return list(range(1, int(row[0]) + 1)) if row else []
