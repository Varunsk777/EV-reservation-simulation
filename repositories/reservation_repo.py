from __future__ import annotations

from datetime import datetime

from psycopg2.extensions import connection as PgConnection


def get_station_slots(conn: PgConnection, station_id: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT slot_id
            FROM core.charging_slots
            WHERE station_id = %s
            ORDER BY slot_id ASC;
            """,
            (station_id,),
        )
        rows = cur.fetchall()

    slot_ids = [int(row[0]) for row in rows]
    print(f"[DEBUG] total slots fetched: {len(slot_ids)}")
    print(f"[DEBUG] slot_ids list: {slot_ids}")
    return slot_ids


def check_conflict(
    conn: PgConnection,
    slot_id: int,
    start: datetime,
    end: datetime,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM core.reservations
            WHERE slot_id = %s
              AND status = 'confirmed'
              AND (%s < scheduled_end AND %s > scheduled_start)
            LIMIT 1;
            """,
            (slot_id, start, end),
        )
        result = cur.fetchall()

    print(f"[DEBUG] DB rows returned for slot {slot_id}: {result}")
    return len(result) > 0


def create_reservation(
    conn: PgConnection,
    vehicle_id: int,
    station_id: int,
    slot_id: int,
    start: datetime,
    end: datetime,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO core.reservations (
                vehicle_id,
                slot_id,
                status,
                reserved_at,
                expires_at,
                scheduled_start,
                scheduled_end,
                station_id
            )
            VALUES (%s, %s, 'confirmed', NOW(), NULL, %s, %s, %s)
            RETURNING reservation_id;
            """,
            (vehicle_id, slot_id, start, end, station_id),
        )
        reservation_id = int(cur.fetchone()[0])

    return reservation_id
