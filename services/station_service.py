from __future__ import annotations

from core.database import get_db_connection
from core.redis import redis_client


def load_stations_into_redis() -> None:
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT station_id, slot_id
            FROM core.charging_slots
            ORDER BY station_id ASC, slot_id ASC;
            """
        )
        rows = cur.fetchall()

        if not rows:
            print("No stations or slots found in the database.")
            return

        for station_id, slot_id in rows:
            redis_client.hset(f"station:{station_id}:slots", str(slot_id), "available")

        print("Stations loaded into Redis.")
    finally:
        cur.close()
        conn.close()


def reset_station_cache() -> None:
    for key in redis_client.keys("station:*:slots"):
        redis_client.delete(key)
    for key in redis_client.keys("station:*:queue"):
        redis_client.delete(key)


def snapshot_stations() -> list[dict[str, object]]:
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT station_id, slot_id
            FROM core.charging_slots
            ORDER BY station_id ASC, slot_id ASC;
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    stations: dict[int, list[dict[str, object]]] = {}

    for station_id, slot_id in rows:
        status = redis_client.hget(f"station:{station_id}:slots", str(slot_id)) or "available"
        stations.setdefault(int(station_id), []).append(
            {
                "slot_id": int(slot_id),
                "status": status,
            }
        )

    return [
        {
            "station_id": station_id,
            "slots": slots,
        }
        for station_id, slots in stations.items()
    ]
