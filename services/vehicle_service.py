from __future__ import annotations

from psycopg2 import errors

from core.database import get_db_connection


def register_vehicle(vehicle: dict[str, object]) -> int | None:
    vehicle_id = str(vehicle["vehicle_id"])
    priority = int(vehicle.get("priority_level", 1))
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            email = f"{vehicle_id.lower()}@simulation.local"
            username = f"user-{vehicle_id}"
            cur.execute(
                """
                SELECT COALESCE(user_id, id)
                FROM core.users
                WHERE vehicle_id = %s OR email = %s OR username = %s
                ORDER BY COALESCE(user_id, id)
                LIMIT 1;
                """,
                (vehicle_id, email, username),
            )
            row = cur.fetchone()
            if row:
                user_id = int(row[0])
                cur.execute(
                    "UPDATE core.users SET priority_level = %s, id = COALESCE(id, user_id) WHERE COALESCE(user_id, id) = %s;",
                    (priority, user_id),
                )
            else:
                try:
                    cur.execute(
                        """
                        INSERT INTO core.users (name, email, password_hash, username, vehicle_id, priority_level)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING user_id;
                        """,
                        (
                            f"user-{vehicle_id}",
                            email,
                            "simulation",
                            username,
                            vehicle_id,
                            priority,
                        ),
                    )
                    user_id = int(cur.fetchone()[0])
                    cur.execute("UPDATE core.users SET id = COALESCE(id, user_id) WHERE user_id = %s;", (user_id,))
                except errors.UniqueViolation:
                    conn.rollback()
                    with conn.cursor() as retry_cur:
                        retry_cur.execute(
                            """
                            SELECT COALESCE(user_id, id)
                            FROM core.users
                            WHERE vehicle_id = %s OR email = %s OR username = %s
                            ORDER BY COALESCE(user_id, id)
                            LIMIT 1;
                            """,
                            (vehicle_id, email, username),
                        )
                        row = retry_cur.fetchone()
                        if not row:
                            return None
                        user_id = int(row[0])
                        retry_cur.execute(
                            "UPDATE core.users SET priority_level = %s, id = COALESCE(id, user_id) WHERE COALESCE(user_id, id) = %s;",
                            (priority, user_id),
                        )
        conn.commit()
        return user_id
    except Exception:
        conn.rollback()
        return None
    finally:
        conn.close()
