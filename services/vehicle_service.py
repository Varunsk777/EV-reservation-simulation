from __future__ import annotations

from core.database import get_db_connection


def register_vehicle(vehicle):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT vehicle_id
            FROM core.vehicles
            WHERE registration_number = %s
            """,
            (f"SIM-{vehicle['vehicle_id']}",),
        )

        existing = cur.fetchone()
        if existing:
            return existing[0]

        cur.execute(
            """
            INSERT INTO core.vehicles (
                user_id,
                registration_number,
                vehicle_type,
                battery_capacity,
                created_at
            )
            VALUES (%s, %s, %s, %s, NOW())
            RETURNING vehicle_id;
            """,
            (
                1,
                f"SIM-{vehicle['vehicle_id']}",
                "car",
                vehicle["battery_needed"],
            ),
        )

        vehicle_id = cur.fetchone()[0]
        conn.commit()
        return vehicle_id

    except Exception as exc:
        conn.rollback()
        print("Vehicle registration error:", exc)
        return None
    finally:
        cur.close()
        conn.close()
