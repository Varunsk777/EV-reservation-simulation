from __future__ import annotations

import json
import asyncio
import logging
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from contextlib import suppress
from typing import Any

from psycopg2 import errors
from psycopg2.extras import RealDictCursor
from redis.exceptions import RedisError

from core.database import db_connection
from core.redis import redis_client


logger = logging.getLogger(__name__)

STATION_LIMITS = {1: 6, 2: 8, 3: 10}
STATION_NAMES = {1: "Station A", 2: "Station B", 3: "Station C"}
EVENT_TYPES = {
    "reservation_created",
    "slot_reserved",
    "charging_started",
    "charging_completed",
    "slot_released",
    "queue_updated",
    "vehicle_waiting",
    "coordinator_decision",
    "reservation_cancelled",
}
LIFECYCLE = ("Searching", "Reserved", "Waiting", "Charging", "Completed")
LOCK_TTL_SECONDS = 240
SEGMENT_MINUTES = 15
HORIZON_HOURS = 3
MIN_TICK_SECONDS = 0.45
BASE_TICK_SECONDS = 3.2


@dataclass
class SimulationClock:
    current_time: datetime
    speed: int = 2
    paused: bool = True


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str
    udt_name: str
    nullable: bool
    default: str | None
    is_identity: bool
    identity_generation: str | None


class OrchestrationService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tick_lock = threading.Lock()
        self._stop = threading.Event()
        self._subscribers: set[Any] = set()
        self._task: asyncio.Task | None = None
        self._bootstrap_task: asyncio.Task | None = None
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._db_columns: dict[tuple[str, str], dict[str, ColumnInfo]] = {}
        self._db_primary_keys: dict[tuple[str, str], list[str]] = {}

    async def initialize(self) -> None:
        """Schedule orchestration bootstrap without blocking FastAPI lifespan."""
        loop = asyncio.get_running_loop()
        self._event_loop = loop
        if self._bootstrap_task and not self._bootstrap_task.done():
            logger.debug("Orchestration bootstrap task already scheduled")
            return
        self._bootstrap_task = loop.create_task(self._bootstrap_orchestration())
        logger.info("Orchestration bootstrap scheduled (non-blocking)")

    async def _bootstrap_orchestration(self) -> None:
        try:
            logger.info("Database startup: introspecting PostgreSQL schema")
            await asyncio.to_thread(self._introspect_database_schema)
            logger.info("Simulation startup: validating Redis connection")
            await asyncio.to_thread(self._verify_redis_connection)
            logger.info("Simulation startup: seeding stations and Redis runtime state")
            await asyncio.to_thread(self._seed_small_station_set)
            await asyncio.to_thread(self.rebuild_redis_state)
            await asyncio.to_thread(
                self._write_clock,
                SimulationClock(current_time=datetime.now(timezone.utc), speed=2, paused=False),
            )
            logger.info("Simulation startup: registering background loop task")
            self._start_background_task()
            logger.info("Simulation startup complete: realtime loop is active")
        except asyncio.CancelledError:
            logger.info("Orchestration bootstrap cancelled during shutdown")
            raise
        except Exception:
            logger.exception("Orchestration bootstrap failed")

    def _introspect_database_schema(self) -> None:
        tables = [
            ("core", "stations"),
            ("core", "users"),
            ("core", "reservations"),
            ("core", "charging_sessions"),
            ("logs", "station_events"),
        ]
        self._db_columns.clear()
        self._db_primary_keys.clear()
        with db_connection() as conn:
            with conn.cursor() as cur:
                for schema, table in tables:
                    key = (schema, table)
                    cur.execute(
                        """
                        SELECT
                            column_name,
                            data_type,
                            udt_name,
                            is_nullable = 'YES',
                            column_default,
                            COALESCE(is_identity, 'NO') = 'YES',
                            identity_generation
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position;
                        """,
                        (schema, table),
                    )
                    cols: dict[str, ColumnInfo] = {}
                    for row in cur.fetchall():
                        cols[row[0]] = ColumnInfo(
                            name=row[0],
                            data_type=row[1],
                            udt_name=row[2],
                            nullable=row[3],
                            default=row[4],
                            is_identity=row[5],
                            identity_generation=row[6],
                        )
                    self._db_columns[key] = cols
                    cur.execute(
                        """
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                        WHERE tc.table_schema = %s
                          AND tc.table_name = %s
                          AND tc.constraint_type = 'PRIMARY KEY'
                        ORDER BY kcu.ordinal_position;
                        """,
                        (schema, table),
                    )
                    self._db_primary_keys[key] = [r[0] for r in cur.fetchall()]
        for (schema, table), pks in self._db_primary_keys.items():
            col_names = list(self._db_columns.get((schema, table), {}).keys())
            logger.info(
                "Schema introspection: %s.%s columns=%s primary_key=%s",
                schema,
                table,
                col_names,
                pks,
            )

    @staticmethod
    def _naive_timestamp(dt: datetime) -> datetime:
        """Map aware UTC datetimes to naive timestamps for legacy without-tz columns."""
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    async def shutdown(self) -> None:
        logger.info("Simulation shutdown requested")
        if self._bootstrap_task and not self._bootstrap_task.done():
            self._bootstrap_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._bootstrap_task
        self._bootstrap_task = None
        self._stop.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except asyncio.TimeoutError:
                logger.warning("Simulation loop did not stop within timeout; cancelling task")
                self._task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._task
        self._task = None
        self._event_loop = None

    def _start_background_task(self) -> None:
        try:
            self._event_loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._event_loop and self._event_loop.is_running():
                logger.info("Simulation loop start requested from worker thread; scheduling on application event loop")
                self._event_loop.call_soon_threadsafe(self._start_background_task)
            return
        if self._task and not self._task.done():
            logger.debug("Simulation loop task is already active")
            return
        self._stop.clear()
        self._task = self._event_loop.create_task(self._run_loop())
        self._task.add_done_callback(self._log_background_task_result)
        logger.info("Simulation loop task created")

    @staticmethod
    def _log_background_task_result(task: asyncio.Task) -> None:
        if task.cancelled():
            logger.info("Simulation loop task cancelled")
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            logger.info("Simulation loop task cancelled")
            return
        if exc:
            logger.error("Simulation loop task crashed", exc_info=(type(exc), exc, exc.__traceback__))
        else:
            logger.info("Simulation loop task stopped cleanly")

    def snapshot(self) -> dict[str, Any]:
        clock = self._read_clock()
        stations = self._station_timeline_snapshot(clock.current_time)
        vehicles = self._vehicle_snapshot()
        events = self._event_snapshot()
        decision = self._latest_decision()
        metrics = self._metrics(stations, vehicles)
        return {
            "clock": {
                "current_time": clock.current_time.isoformat(),
                "speed": clock.speed,
                "paused": clock.paused,
                "running": self.is_running(),
            },
            "metrics": metrics,
            "stations": stations,
            "vehicles": vehicles,
            "decision": decision,
            "events": events,
        }

    def start(self, speed: int | None = None) -> bool:
        if speed is not None:
            self.set_speed(speed)
        with self._lock:
            was_running = self.is_running()
            if not self._task or self._task.done():
                self._start_background_task()
            self._stop.clear()
            clock = self._read_clock()
            clock.paused = False
            self._write_clock(clock)
            logger.info("Simulation start requested: speed=%s running_task=%s", clock.speed, bool(self._task and not self._task.done()))
            self._publish_snapshot()
            return not was_running

    def pause(self) -> None:
        clock = self._read_clock()
        clock.paused = True
        self._write_clock(clock)
        logger.info("Simulation paused")
        self._publish_snapshot()

    def resume(self) -> None:
        clock = self._read_clock()
        clock.paused = False
        self._write_clock(clock)
        logger.info("Simulation resumed")
        self._publish_snapshot()

    def set_speed(self, speed: int) -> None:
        clock = self._read_clock()
        clock.speed = max(1, min(int(speed), 20))
        self._write_clock(clock)
        logger.info("Simulation speed changed: speed=%s", clock.speed)

    def reset(self) -> None:
        self.pause()
        with self._tick_lock:
            self._clear_runtime_keys()
            with db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM core.charging_sessions;")
                    cur.execute("DELETE FROM core.reservations;")
                    cur.execute("DELETE FROM logs.station_events;")
                    cur.execute("DELETE FROM core.users WHERE username LIKE 'sim-user-%';")
                conn.commit()
            self._seed_small_station_set()
            self.rebuild_redis_state()
            self._write_clock(SimulationClock(current_time=datetime.now(timezone.utc), speed=2, paused=True))
        self._start_background_task()
        self._publish_snapshot()

    def is_running(self) -> bool:
        return bool(self._task and not self._task.done() and not self._read_clock().paused)

    async def register_websocket(self, websocket) -> None:
        self._subscribers.add(websocket)
        logger.info("Websocket connected: subscribers=%s", len(self._subscribers))
        await websocket.send_json({"type": "snapshot", "payload": self.snapshot()})

    def unregister_websocket(self, websocket) -> None:
        self._subscribers.discard(websocket)
        logger.info("Websocket disconnected: subscribers=%s", len(self._subscribers))

    async def _run_loop(self) -> None:
        logger.info("Simulation loop started")
        while not self._stop.is_set():
            clock = self._read_clock()
            if clock.paused:
                await asyncio.sleep(0.35)
                continue
            try:
                await asyncio.to_thread(self._run_tick)
            except Exception as exc:
                logger.exception("Simulation tick failed")
                try:
                    self._publish_event(
                        station_id=None,
                        vehicle_id=None,
                        event_type="queue_updated",
                        message=f"Simulation tick recovered after {type(exc).__name__}: {exc}",
                    )
                except Exception:
                    logger.exception("Failed to publish simulation failure event")
            refreshed = self._read_clock()
            await asyncio.sleep(self._tick_delay(refreshed.speed))
        logger.info("Simulation loop exiting")

    def _run_tick(self) -> None:
        with self._tick_lock:
            clock = self._read_clock()
            if clock.paused:
                return
            clock.current_time = clock.current_time + timedelta(minutes=self._tick_minutes(clock.speed))
            self._write_clock(clock)
            logger.info("Simulation tick: sim_time=%s speed=%s", clock.current_time.isoformat(), clock.speed)
            self._advance_lifecycles(clock.current_time)
            request_count = self._requests_per_tick(clock.speed)
            logger.info("Vehicle generation loop: creating %s request(s)", request_count)
            for _ in range(request_count):
                self._generate_vehicle_request(clock.current_time)
            self._advance_lifecycles(clock.current_time)
            self._publish_snapshot()

    @staticmethod
    def _tick_minutes(speed: int) -> int:
        return max(3, min(18, 3 + int(speed) * 2))

    @staticmethod
    def _tick_delay(speed: int) -> float:
        return max(MIN_TICK_SECONDS, BASE_TICK_SECONDS / max(1, int(speed)))

    @staticmethod
    def _requests_per_tick(speed: int) -> int:
        base = 2 + min(4, int(speed) // 2)
        jitter = random.randint(0, 3)
        surge = random.randint(2, 5) if random.random() < min(0.35, 0.08 + int(speed) * 0.015) else 0
        return base + jitter + surge

    def _seed_small_station_set(self) -> None:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.stations
                    SET station_name = CASE id WHEN 1 THEN 'Station A' WHEN 2 THEN 'Station B' WHEN 3 THEN 'Station C' ELSE station_name END,
                        name = CASE id WHEN 1 THEN 'Station A' WHEN 2 THEN 'Station B' WHEN 3 THEN 'Station C' ELSE COALESCE(name, station_name) END,
                        location = CASE id
                            WHEN 1 THEN 'North coordination zone'
                            WHEN 2 THEN 'Central coordination zone'
                            WHEN 3 THEN 'South coordination zone'
                            ELSE location
                        END,
                        charger_count = CASE id WHEN 1 THEN 6 WHEN 2 THEN 8 WHEN 3 THEN 10 ELSE charger_count END,
                        total_slots = CASE id WHEN 1 THEN 6 WHEN 2 THEN 8 WHEN 3 THEN 10 ELSE COALESCE(total_slots, charger_count) END,
                        status = 'online'
                    WHERE id IN (1, 2, 3);
                    """
                )
                cur.execute(
                    """
                    INSERT INTO core.stations (id, name, station_name, location, charger_count, total_slots, status)
                    SELECT id, station_name, station_name, location, charger_count, charger_count, status
                    FROM (
                        VALUES
                            (1, 'Station A', 'North coordination zone', 6, 'online'),
                            (2, 'Station B', 'Central coordination zone', 8, 'online'),
                            (3, 'Station C', 'South coordination zone', 10, 'online')
                    ) AS seed(id, station_name, location, charger_count, status)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM core.stations existing WHERE existing.id = seed.id
                    );
                    """
                )
            conn.commit()

    def rebuild_redis_state(self) -> None:
        self._clear_runtime_keys()
        for station_id, charger_count in STATION_LIMITS.items():
            live_key = f"station:{station_id}:slots:live"
            for charger_id in range(1, charger_count + 1):
                self._redis_hset(live_key, str(charger_id), "free")
            self._write_station_timeline(station_id, [])
        self._hydrate_from_postgres()

    def _hydrate_from_postgres(self) -> None:
        now = self._read_clock().current_time
        horizon = now + timedelta(hours=HORIZON_HOURS)
        rows = self._reservation_rows(now - timedelta(hours=1), horizon)
        intervals_by_station: dict[int, list[dict[str, Any]]] = {station_id: [] for station_id in STATION_LIMITS}
        for row in rows:
            status = self._interval_status(row, now)
            interval = {
                "reservation_id": row["id"],
                "charger_id": row["charger_id"],
                "vehicle_id": row["vehicle_id"],
                "start": row["reservation_start"].isoformat(),
                "end": row["reservation_end"].isoformat(),
                "status": status,
            }
            intervals_by_station.setdefault(row["station_id"], []).append(interval)
            if status in {"reserved", "charging"}:
                self._redis_hset(f"station:{row['station_id']}:slots:live", str(row["charger_id"]), status)
        for station_id, intervals in intervals_by_station.items():
            self._write_station_timeline(station_id, intervals)

    def _station_timeline_snapshot(self, now: datetime) -> list[dict[str, Any]]:
        self._hydrate_from_postgres()
        stations = []
        ticks = [now + timedelta(minutes=30 * i) for i in range(0, HORIZON_HOURS * 2 + 1)]
        for station_id, charger_count in STATION_LIMITS.items():
            intervals = self._read_station_timeline(station_id)
            live = self._redis_hgetall(f"station:{station_id}:slots:live")
            chargers = []
            for charger_id in range(1, charger_count + 1):
                segments = self._build_segments(charger_id, now, intervals)
                chargers.append(
                    {
                        "charger_id": charger_id,
                        "state": live.get(str(charger_id), "free"),
                        "segments": segments,
                    }
                )
            free_windows = self._free_windows(chargers)
            stations.append(
                {
                    "station_id": station_id,
                    "station_name": STATION_NAMES[station_id],
                    "location": self._station_location(station_id),
                    "charger_count": charger_count,
                    "status": "online",
                    "time_axis": [tick.isoformat() for tick in ticks],
                    "chargers": chargers,
                    "free_windows": free_windows[:8],
                }
            )
        return stations

    def _build_segments(self, charger_id: int, now: datetime, intervals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        segments = []
        for index in range((HORIZON_HOURS * 60) // SEGMENT_MINUTES):
            start = now + timedelta(minutes=index * SEGMENT_MINUTES)
            end = start + timedelta(minutes=SEGMENT_MINUTES)
            overlap = next(
                (
                    item
                    for item in intervals
                    if int(item["charger_id"]) == charger_id
                    and datetime.fromisoformat(item["start"]) < end
                    and datetime.fromisoformat(item["end"]) > start
                ),
                None,
            )
            status = overlap["status"] if overlap else "available"
            segments.append(
                {
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "status": status,
                    "vehicle_id": overlap.get("vehicle_id") if overlap else None,
                }
            )
        return segments

    def _free_windows(self, chargers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        windows = []
        for charger in chargers:
            run_start = None
            run_end = None
            for segment in charger["segments"]:
                if segment["status"] == "available":
                    run_start = run_start or segment["start"]
                    run_end = segment["end"]
                elif run_start:
                    windows.append({"charger_id": charger["charger_id"], "start": run_start, "end": run_end})
                    run_start = None
            if run_start:
                windows.append({"charger_id": charger["charger_id"], "start": run_start, "end": run_end})
        return windows

    def _generate_vehicle_request(self, now: datetime) -> None:
        vehicle_id = f"EV-{int(time.time() * 1000)}-{random.randint(100, 999)}"
        priority = random.choice([1, 1, 2, 2, 3])
        duration = timedelta(minutes=random.choice([30, 30, 45, 45, 60, 75, 90]))
        earliest = now + timedelta(minutes=random.choice([0, 0, 5, 10, 15, 20, 30, 45]))
        logger.info(
            "Vehicle created: vehicle_id=%s priority=%s earliest=%s duration_minutes=%s",
            vehicle_id,
            priority,
            earliest.isoformat(),
            int(duration.total_seconds() / 60),
        )
        user_id = self._ensure_user(vehicle_id, priority)
        decision = self._choose_interval(vehicle_id, priority, earliest, duration)
        logger.info(
            "Coordinator decision: vehicle_id=%s selected_station=%s allocation=%s",
            vehicle_id,
            decision.get("selected_station"),
            decision.get("allocation"),
        )
        self._redis_set_json("coordinator:latest_decision", decision)
        self._publish_event(
            station_id=decision.get("selected_station"),
            vehicle_id=vehicle_id,
            event_type="coordinator_decision",
            message=decision["reasoning"],
        )
        if not decision.get("allocation"):
            self._redis_set_json(
                f"vehicle:{vehicle_id}:state",
                {"status": "Searching", "vehicle_id": vehicle_id, "soc": random.randint(18, 62), "priority": priority},
            )
            self._publish_event(
                station_id=None,
                vehicle_id=vehicle_id,
                event_type="queue_updated",
                message=f"{vehicle_id} added to the search queue; no fit in the active horizon.",
            )
            return
        allocation = decision["allocation"]
        lock_key = (
            f"lock:{allocation['station_id']}:{allocation['charger_id']}:"
            f"{allocation['start'].replace(':', '').replace('+', '_')}"
        )
        if not self._redis_set(lock_key, vehicle_id, nx=True, ex=LOCK_TTL_SECONDS):
            self._publish_event(
                station_id=allocation["station_id"],
                vehicle_id=vehicle_id,
                event_type="queue_updated",
                message=f"{vehicle_id} queued because a temporary slot lock already exists.",
            )
            return
        reservation_id = self._insert_reservation(
            user_id=user_id,
            vehicle_id=vehicle_id,
            station_id=allocation["station_id"],
            charger_id=allocation["charger_id"],
            start=datetime.fromisoformat(allocation["start"]),
            end=datetime.fromisoformat(allocation["end"]),
        )
        if reservation_id is None:
            self._redis_delete(lock_key)
            self._redis_set_json(
                f"vehicle:{vehicle_id}:state",
                {"status": "Searching", "vehicle_id": vehicle_id, "soc": random.randint(18, 62), "priority": priority},
            )
            self._publish_event(
                station_id=allocation["station_id"],
                vehicle_id=vehicle_id,
                event_type="queue_updated",
                message=f"{vehicle_id} queued because the selected slot was already reserved.",
            )
            return
        logger.info(
            "Reservation created: reservation_id=%s vehicle_id=%s station_id=%s charger_id=%s",
            reservation_id,
            vehicle_id,
            allocation["station_id"],
            allocation["charger_id"],
        )
        self._hydrate_from_postgres()
        self._redis_set_json(
            f"vehicle:{vehicle_id}:state",
            {
                "status": "Reserved",
                "vehicle_id": vehicle_id,
                "assigned_station": allocation["station_id"],
                "assigned_charger": allocation["charger_id"],
                "reservation_id": reservation_id,
                "soc": random.randint(18, 62),
                "priority": priority,
            },
        )
        self._publish_event(
            station_id=allocation["station_id"],
            vehicle_id=vehicle_id,
            event_type="reservation_created",
            message=f"{vehicle_id} reserved Station {allocation['station_id']} charger {allocation['charger_id']}.",
        )
        self._publish_event(
            station_id=allocation["station_id"],
            vehicle_id=vehicle_id,
            event_type="slot_reserved",
            message=f"Future interval locked for {vehicle_id}.",
        )

    def _choose_interval(self, vehicle_id: str, priority: int, earliest: datetime, duration: timedelta) -> dict[str, Any]:
        candidate_stations = []
        best = None
        for station_id, charger_count in STATION_LIMITS.items():
            intervals = self._read_station_timeline(station_id)
            allocation = self._find_station_window(station_id, charger_count, intervals, earliest, duration)
            wait_minutes = (
                int((datetime.fromisoformat(allocation["start"]) - earliest).total_seconds() / 60)
                if allocation
                else 999
            )
            load = len([item for item in intervals if item["status"] in {"reserved", "charging"}])
            score = wait_minutes + load * 4 - priority * 3
            candidate = {
                "station_id": station_id,
                "station_name": STATION_NAMES[station_id],
                "wait_minutes": wait_minutes,
                "active_intervals": load,
                "score": score,
                "allocation": allocation,
            }
            candidate_stations.append(candidate)
            if allocation and (best is None or score < best["score"]):
                best = candidate
        reasoning = (
            f"{vehicle_id} assigned to {STATION_NAMES[best['station_id']]} because it has the lowest "
            f"combined wait/load score ({best['score']})."
            if best
            else f"{vehicle_id} remains searching because no reusable window fits the requested duration."
        )
        return {
            "vehicle_id": vehicle_id,
            "priority": priority,
            "candidate_stations": candidate_stations,
            "selected_station": best["station_id"] if best else None,
            "allocation": best["allocation"] if best else None,
            "reasoning": reasoning,
            "decided_at": datetime.now(timezone.utc).isoformat(),
        }

    def _find_station_window(
        self,
        station_id: int,
        charger_count: int,
        intervals: list[dict[str, Any]],
        earliest: datetime,
        duration: timedelta,
    ) -> dict[str, Any] | None:
        for offset in range(0, HORIZON_HOURS * 60, SEGMENT_MINUTES):
            start = earliest + timedelta(minutes=offset)
            end = start + duration
            if end > earliest + timedelta(hours=HORIZON_HOURS):
                return None
            for charger_id in range(1, charger_count + 1):
                has_conflict = any(
                    int(item["charger_id"]) == charger_id
                    and datetime.fromisoformat(item["start"]) < end
                    and datetime.fromisoformat(item["end"]) > start
                    and item["status"] in {"reserved", "charging"}
                    for item in intervals
                )
                if not has_conflict and not self._db_conflict(station_id, charger_id, start, end):
                    return {
                        "station_id": station_id,
                        "charger_id": charger_id,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                    }
        return None

    def _advance_lifecycles(self, now: datetime) -> None:
        rows = self._reservation_rows(now - timedelta(hours=2), now + timedelta(hours=HORIZON_HOURS))
        for row in rows:
            vehicle_id = row["vehicle_id"]
            if row["reservation_start"] <= now < row["reservation_end"]:
                if row["reservation_status"] != "charging":
                    self._update_reservation_status(row["id"], "charging")
                    self._start_session(row, now)
                    self._redis_hset(f"station:{row['station_id']}:slots:live", str(row["charger_id"]), "charging")
                    self._set_vehicle_status(vehicle_id, "Charging", row)
                    logger.info(
                        "Charging lifecycle transition: reservation_id=%s vehicle_id=%s status=charging",
                        row["id"],
                        vehicle_id,
                    )
                    self._publish_event(row["station_id"], vehicle_id, "charging_started", f"{vehicle_id} started charging.")
            elif now >= row["reservation_end"] and row["reservation_status"] != "completed":
                self._update_reservation_status(row["id"], "completed")
                self._complete_session(row, now)
                self._redis_hset(f"station:{row['station_id']}:slots:live", str(row["charger_id"]), "free")
                self._set_vehicle_status(vehicle_id, "Completed", row)
                logger.info(
                    "Charging lifecycle transition: reservation_id=%s vehicle_id=%s status=completed",
                    row["id"],
                    vehicle_id,
                )
                self._publish_event(row["station_id"], vehicle_id, "charging_completed", f"{vehicle_id} completed charging.")
                self._publish_event(row["station_id"], vehicle_id, "slot_released", f"Charger {row['charger_id']} released.")
            elif row["reservation_start"] - timedelta(minutes=15) <= now < row["reservation_start"] and row["reservation_status"] != "waiting":
                self._update_reservation_status(row["id"], "waiting")
                self._set_vehicle_status(vehicle_id, "Waiting", row)
                logger.info(
                    "Charging lifecycle transition: reservation_id=%s vehicle_id=%s status=waiting",
                    row["id"],
                    vehicle_id,
                )
                self._publish_event(row["station_id"], vehicle_id, "vehicle_waiting", f"{vehicle_id} is waiting for its interval.")

    def _ensure_user(self, vehicle_id: str, priority: int) -> int:
        payload = {
            "name": vehicle_id,
            "email": f"{vehicle_id.lower()}@simulation.local",
            "username": f"sim-user-{vehicle_id}",
            "vehicle_id": vehicle_id,
            "priority_level": priority,
        }
        logger.info("PostgreSQL users insert payload=%s", payload)
        with db_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO core.users (
                            name,
                            email,
                            password_hash,
                            username,
                            vehicle_id,
                            priority_level
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING user_id;
                        """,
                        (
                            payload["name"],
                            payload["email"],
                            "simulation-only",
                            payload["username"],
                            payload["vehicle_id"],
                            payload["priority_level"],
                        ),
                    )
                    row = cur.fetchone()
                    if not row:
                        raise RuntimeError("INSERT INTO core.users returned no row")
                    user_id = int(row[0])
                    cur.execute(
                        "UPDATE core.users SET id = user_id WHERE user_id = %s;",
                        (user_id,),
                    )
                except Exception:
                    logger.exception("PostgreSQL users insert failed; rolling back")
                    conn.rollback()
                    raise
            conn.commit()
        logger.info("PostgreSQL users commit OK user_id=%s", user_id)
        return user_id

    def _insert_reservation(
        self,
        user_id: int,
        vehicle_id: str,
        station_id: int,
        charger_id: int,
        start: datetime,
        end: datetime,
    ) -> int | None:
        legacy_slot_id = station_id * 100 + charger_id
        sched_start = self._naive_timestamp(start)
        sched_end = self._naive_timestamp(end)
        expires_at = self._naive_timestamp(end)
        insert_sql = """
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
                'reserved', CURRENT_TIMESTAMP,
                NULL
            )
            RETURNING reservation_id;
        """
        insert_payload = {
            "slot_id": legacy_slot_id,
            "station_id": station_id,
            "expires_at": expires_at,
            "scheduled_start": sched_start,
            "scheduled_end": sched_end,
            "point_id": charger_id,
            "user_id": user_id,
            "charger_id": charger_id,
            "reservation_start": start,
            "reservation_end": end,
            "vehicle_id_tag": vehicle_id,
        }
        logger.info("PostgreSQL reservations insert: payload=%s", insert_payload)
        with db_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        insert_sql,
                        (
                            legacy_slot_id,
                            station_id,
                            expires_at,
                            sched_start,
                            sched_end,
                            charger_id,
                            user_id,
                            charger_id,
                            start,
                            end,
                        ),
                    )
                    row = cur.fetchone()
                    if not row:
                        logger.error("PostgreSQL reservations insert returned no reservation_id")
                        conn.rollback()
                        return None
                    reservation_id = int(row[0])
                    sync_sql = "UPDATE core.reservations SET id = reservation_id WHERE reservation_id = %s;"
                    cur.execute(sync_sql, (reservation_id,))
                    logger.debug("PostgreSQL reservations sync id: sql=%s reservation_id=%s", sync_sql, reservation_id)
                except errors.ExclusionViolation as exc:
                    logger.warning(
                        "PostgreSQL reservations excluded by overlap constraint: %s payload=%s",
                        exc,
                        insert_payload,
                    )
                    conn.rollback()
                    return None
                except Exception:
                    logger.exception("PostgreSQL reservations insert failed payload=%s", insert_payload)
                    conn.rollback()
                    return None
            conn.commit()
        logger.info("PostgreSQL reservations commit OK reservation_id=%s", reservation_id)
        return reservation_id

    def _reservation_rows(self, start: datetime, end: datetime) -> list[dict[str, Any]]:
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        COALESCE(r.id, r.reservation_id) AS id,
                        r.reservation_id,
                        r.user_id,
                        r.station_id,
                        r.charger_id,
                        r.reservation_start,
                        r.reservation_end,
                        r.reservation_status,
                        u.vehicle_id
                    FROM core.reservations r
                    LEFT JOIN core.users u ON u.user_id = r.user_id
                    WHERE r.station_id IN (1, 2, 3)
                      AND r.reservation_start < %s
                      AND r.reservation_end > %s
                      AND r.reservation_status <> 'cancelled'
                    ORDER BY r.reservation_start ASC, r.reservation_id ASC;
                    """,
                    (end, start),
                )
                return [dict(row) for row in cur.fetchall()]

    def _db_conflict(self, station_id: int, charger_id: int, start: datetime, end: datetime) -> bool:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM core.reservations
                        WHERE station_id = %s
                          AND charger_id = %s
                          AND reservation_status IN ('reserved', 'waiting', 'charging')
                          AND reservation_start < %s
                          AND reservation_end > %s
                    );
                    """,
                    (station_id, charger_id, end, start),
                )
                return bool(cur.fetchone()[0])

    def _start_session(self, row: dict[str, Any], now: datetime) -> None:
        res_id = int(row["id"])
        started_at = self._naive_timestamp(now)
        insert_sql = """
            INSERT INTO core.charging_sessions (
                reservation_id,
                station_id,
                charger_id,
                vehicle_id,
                started_at,
                status,
                point_id,
                session_start,
                session_status
            )
            SELECT %s, %s, %s, %s, %s, 'charging', %s, %s, 'charging'
            WHERE NOT EXISTS (
                SELECT 1 FROM core.charging_sessions
                WHERE reservation_id = %s AND session_status = 'charging'
            )
            RETURNING session_id;
        """
        payload = {
            "reservation_id": res_id,
            "station_id": row["station_id"],
            "charger_id": row["charger_id"],
            "vehicle_id": row["vehicle_id"],
            "started_at": started_at,
            "session_start": now,
        }
        logger.info("PostgreSQL charging_sessions insert: sql=%s payload=%s", insert_sql.strip(), payload)
        with db_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        insert_sql,
                        (
                            res_id,
                            row["station_id"],
                            row["charger_id"],
                            row["vehicle_id"],
                            started_at,
                            row["charger_id"],
                            now,
                            res_id,
                        ),
                    )
                    out = cur.fetchone()
                    if out:
                        session_id = int(out[0])
                        cur.execute(
                            "UPDATE core.charging_sessions SET id = session_id WHERE session_id = %s;",
                            (session_id,),
                        )
                        logger.debug("PostgreSQL charging_sessions id synced session_id=%s", session_id)
                    else:
                        logger.debug(
                            "PostgreSQL charging_sessions skip insert (already charging) reservation_id=%s",
                            res_id,
                        )
                except Exception:
                    logger.exception("PostgreSQL charging_sessions insert failed payload=%s", payload)
                    conn.rollback()
                    raise
            conn.commit()
        logger.info("PostgreSQL charging_sessions commit OK reservation_id=%s", res_id)

    def _complete_session(self, row: dict[str, Any], now: datetime) -> None:
        res_id = int(row["id"])
        ended_at = self._naive_timestamp(now)
        update_sql = """
            UPDATE core.charging_sessions
            SET session_end = %s,
                session_status = 'completed',
                ended_at = %s,
                status = 'completed'
            WHERE reservation_id = %s AND session_status = 'charging';
        """
        logger.info(
            "PostgreSQL charging_sessions update complete: sql=%s reservation_id=%s session_end=%s",
            update_sql.strip(),
            res_id,
            now,
        )
        with db_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        update_sql,
                        (now, ended_at, res_id),
                    )
                except Exception:
                    logger.exception("PostgreSQL charging_sessions update failed reservation_id=%s", res_id)
                    conn.rollback()
                    raise
            conn.commit()
        logger.info("PostgreSQL charging_sessions commit OK (complete) reservation_id=%s", res_id)

    def _update_reservation_status(self, reservation_id: int, status: str) -> None:
        legacy_booking_status = {
            "reserved": "CONFIRMED",
            "waiting": "CONFIRMED",
            "charging": "ACTIVE",
            "completed": "COMPLETED",
            "cancelled": "CANCELLED",
        }.get(status, status.upper())
        status_text_column = {
            "reserved": "confirmed",
            "waiting": "confirmed",
            "charging": "charging",
            "completed": "completed",
            "cancelled": "cancelled",
        }.get(status, "confirmed")
        update_sql = """
            UPDATE core.reservations
            SET reservation_status = %s,
                booking_status = %s,
                status = %s
            WHERE reservation_id = %s AND reservation_status IS DISTINCT FROM %s;
        """
        logger.info(
            "PostgreSQL reservations status update: sql=%s reservation_id=%s reservation_status=%s status=%s booking=%s",
            update_sql.strip(),
            reservation_id,
            status,
            status_text_column,
            legacy_booking_status,
        )
        with db_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        update_sql,
                        (status, legacy_booking_status, status_text_column, reservation_id, status),
                    )
                except Exception:
                    logger.exception("PostgreSQL reservations status update failed reservation_id=%s", reservation_id)
                    conn.rollback()
                    raise
            conn.commit()
        logger.info("PostgreSQL reservations status commit OK reservation_id=%s", reservation_id)

    def _set_vehicle_status(self, vehicle_id: str, status: str, row: dict[str, Any]) -> None:
        payload = {
            "status": status,
            "vehicle_id": vehicle_id,
            "assigned_station": row["station_id"],
            "assigned_charger": row["charger_id"],
            "reservation_id": row["id"],
        }
        self._redis_set_json(f"vehicle:{vehicle_id}:state", payload)

    def _vehicle_snapshot(self) -> list[dict[str, Any]]:
        vehicles = []
        for key in self._redis_keys("vehicle:*:state"):
            value = self._redis_get_json(key)
            if value:
                vehicles.append(value)
        status_order = {name: index for index, name in enumerate(LIFECYCLE)}
        return sorted(vehicles, key=lambda item: (status_order.get(item.get("status"), 99), item.get("vehicle_id", "")))[:80]

    def _event_snapshot(self) -> list[dict[str, Any]]:
        try:
            rows = redis_client.xrevrange("events:stream", count=80)
            return [{**payload, "id": event_id} for event_id, payload in rows]
        except RedisError:
            logger.exception("Redis event stream read failed; falling back to Postgres event log")
            with db_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT station_id, vehicle_id, event_type, event_message, event_timestamp
                        FROM logs.station_events
                        ORDER BY event_timestamp DESC
                        LIMIT 80;
                        """
                    )
                    return [
                        {
                            "station_id": row["station_id"],
                            "vehicle_id": row["vehicle_id"],
                            "event_type": row["event_type"],
                            "message": row["event_message"],
                            "timestamp": row["event_timestamp"].isoformat(),
                        }
                        for row in cur.fetchall()
                    ]

    def _publish_event(self, station_id: int | None, vehicle_id: str | None, event_type: str, message: str) -> None:
        if event_type not in EVENT_TYPES:
            event_type = "queue_updated"
        timestamp = datetime.now(timezone.utc)
        payload = {
            "station_id": "" if station_id is None else str(station_id),
            "vehicle_id": vehicle_id or "",
            "event_type": event_type,
            "message": message,
            "timestamp": timestamp.isoformat(),
        }
        try:
            redis_client.xadd("events:stream", payload, maxlen=500, approximate=True)
            logger.info("Event stream publish: type=%s vehicle_id=%s station_id=%s", event_type, vehicle_id, station_id)
        except RedisError:
            logger.exception("Redis event stream publish failed")
            raise
        insert_sql = """
            INSERT INTO logs.station_events (
                station_id, slot_id, vehicle_id, event_type, event_message, event_timestamp
            )
            VALUES (%s, NULL, %s, %s, %s, %s)
            RETURNING event_id;
        """
        db_payload = {
            "station_id": station_id,
            "vehicle_id": vehicle_id,
            "event_type": event_type,
            "event_message": message,
            "event_timestamp": timestamp,
        }
        logger.debug("PostgreSQL station_events insert: sql=%s payload=%s", insert_sql.strip(), db_payload)
        with db_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        insert_sql,
                        (station_id, vehicle_id, event_type, message, timestamp),
                    )
                    row = cur.fetchone()
                    if row:
                        event_id = int(row[0])
                        cur.execute(
                            "UPDATE logs.station_events SET id = event_id WHERE event_id = %s;",
                            (event_id,),
                        )
                except Exception:
                    logger.exception("PostgreSQL station_events insert failed payload=%s", db_payload)
                    conn.rollback()
                    raise
            conn.commit()
        logger.debug("PostgreSQL station_events commit OK")

    def _publish_snapshot(self) -> None:
        payload = {"type": "snapshot", "payload": self.snapshot()}
        stale = []
        broadcast_count = 0
        for websocket in list(self._subscribers):
            try:
                running_loop = None
                with suppress(RuntimeError):
                    running_loop = asyncio.get_running_loop()
                if self._event_loop and self._event_loop.is_running() and running_loop is not self._event_loop:
                    future = asyncio.run_coroutine_threadsafe(websocket.send_json(payload), self._event_loop)
                    future.result(timeout=0.75)
                elif running_loop is self._event_loop:
                    running_loop.create_task(websocket.send_json(payload))
                else:
                    import anyio

                    anyio.from_thread.run(websocket.send_json, payload)
                broadcast_count += 1
            except Exception:
                logger.exception("Websocket broadcast failed; dropping stale subscriber")
                stale.append(websocket)
        for websocket in stale:
            self.unregister_websocket(websocket)
        logger.info("Websocket broadcast complete: delivered=%s stale=%s", broadcast_count, len(stale))

    def _metrics(self, stations: list[dict[str, Any]], vehicles: list[dict[str, Any]]) -> dict[str, int]:
        active_sessions = 0
        pending = 0
        free_windows = 0
        for station in stations:
            free_windows += len(station["free_windows"])
            for charger in station["chargers"]:
                if charger["state"] == "charging":
                    active_sessions += 1
                if charger["state"] == "reserved":
                    pending += 1
        metrics = {
            "active_vehicles": len([v for v in vehicles if v.get("status") != "Completed"]),
            "active_sessions": active_sessions,
            "pending_reservations": pending,
            "free_windows": free_windows,
        }
        self._redis_hset("sim:metrics", mapping={key: str(value) for key, value in metrics.items()})
        return metrics

    def _latest_decision(self) -> dict[str, Any]:
        return self._redis_get_json("coordinator:latest_decision") or {
            "candidate_stations": [],
            "selected_station": None,
            "reasoning": "Waiting for the next vehicle request.",
        }

    def _station_location(self, station_id: int) -> str:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT location FROM core.stations WHERE station_id = %s;", (station_id,))
                row = cur.fetchone()
        return row[0] if row else ""

    @staticmethod
    def _interval_status(row: dict[str, Any], now: datetime) -> str:
        if str(row["reservation_status"]).lower() == "completed":
            return "available"
        if row["reservation_start"] <= now < row["reservation_end"]:
            return "charging"
        return "reserved"

    def _read_clock(self) -> SimulationClock:
        payload = self._redis_get_json("sim:clock")
        if not payload:
            return SimulationClock(current_time=datetime.now(timezone.utc), speed=2, paused=True)
        return SimulationClock(
            current_time=datetime.fromisoformat(payload["current_time"]),
            speed=int(payload.get("speed", 2)),
            paused=bool(payload.get("paused", True)),
        )

    def _write_clock(self, clock: SimulationClock) -> None:
        self._redis_set_json(
            "sim:clock",
            {"current_time": clock.current_time.isoformat(), "speed": clock.speed, "paused": clock.paused},
        )

    def _write_station_timeline(self, station_id: int, intervals: list[dict[str, Any]]) -> None:
        self._redis_set_json(f"station:{station_id}:timeline", intervals)

    def _read_station_timeline(self, station_id: int) -> list[dict[str, Any]]:
        return self._redis_get_json(f"station:{station_id}:timeline") or []

    def _clear_runtime_keys(self) -> None:
        for pattern in ("station:*:timeline", "station:*:slots:live", "vehicle:*:state", "lock:*", "events:stream", "sim:*", "coordinator:*"):
            for key in self._redis_keys(pattern):
                self._redis_delete(key)

    @staticmethod
    def _verify_redis_connection() -> None:
        try:
            redis_client.ping()
            logger.info("Redis connection verified")
        except RedisError:
            logger.exception("Redis connection failed")
            raise

    @staticmethod
    def _redis_keys(pattern: str) -> list[str]:
        try:
            return list(redis_client.keys(pattern))
        except RedisError:
            logger.exception("Redis keys failed: pattern=%s", pattern)
            raise

    @staticmethod
    def _redis_hgetall(key: str) -> dict[str, str]:
        try:
            return redis_client.hgetall(key) or {}
        except RedisError:
            logger.exception("Redis hgetall failed: key=%s", key)
            raise

    @staticmethod
    def _redis_hset(key: str, field: str | None = None, value: str | None = None, mapping: dict[str, str] | None = None) -> None:
        try:
            if mapping is not None:
                redis_client.hset(key, mapping=mapping)
                logger.debug("Redis hset mapping: key=%s fields=%s", key, list(mapping))
            elif field is not None:
                redis_client.hset(key, field, value)
                logger.debug("Redis hset: key=%s field=%s value=%s", key, field, value)
        except RedisError:
            logger.exception("Redis hset failed: key=%s", key)
            raise

    @staticmethod
    def _redis_set(key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool:
        try:
            return bool(redis_client.set(key, value, nx=nx, ex=ex))
        except RedisError:
            logger.exception("Redis set failed: key=%s", key)
            raise

    @staticmethod
    def _redis_delete(key: str) -> None:
        try:
            redis_client.delete(key)
        except RedisError:
            logger.exception("Redis delete failed: key=%s", key)
            raise

    @staticmethod
    def _redis_set_json(key: str, value: Any) -> None:
        try:
            redis_client.set(key, json.dumps(value, default=str))
            logger.debug("Redis JSON write: key=%s", key)
        except RedisError:
            logger.exception("Redis JSON write failed: key=%s", key)
            raise

    @staticmethod
    def _redis_get_json(key: str) -> Any:
        try:
            raw = redis_client.get(key)
            return json.loads(raw) if raw else None
        except (RedisError, json.JSONDecodeError, TypeError):
            logger.exception("Redis JSON read failed: key=%s", key)
            raise


orchestration_service = OrchestrationService()
