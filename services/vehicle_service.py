from __future__ import annotations

from core.database import get_db_connection


def register_vehicle(vehicle: dict[str, object]) -> int | None:
    vehicle_id = str(vehicle["vehicle_id"])
    priority = int(vehicle.get("priority_level", 1))
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM core.users WHERE vehicle_id = %s LIMIT 1;", (vehicle_id,))
            row = cur.fetchone()
            if row:
                return int(row[0])
            cur.execute(
                """
                INSERT INTO core.users (name, email, password_hash, username, vehicle_id, priority_level)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING user_id;
                """,
                (
                    f"user-{vehicle_id}",
                    f"{vehicle_id.lower()}@simulation.local",
                    "simulation",
                    f"user-{vehicle_id}",
                    vehicle_id,
                    priority,
                ),
            )
            user_id = int(cur.fetchone()[0])
            cur.execute("UPDATE core.users SET id = user_id WHERE user_id = %s;", (user_id,))
        conn.commit()
        return user_id
    except Exception:
        conn.rollback()
        return None
    finally:
        conn.close()
