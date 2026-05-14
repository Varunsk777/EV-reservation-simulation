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
    "conflict_detected",
    "rerouting",
    "queue_overflow",
    "priority_preempted",
    "allocating",
    "conflict_escalating",
    "allocation_retry",
    "station_congestion",
    "adaptive_allocation",
}
LIFECYCLE = ("Searching", "Reserved", "Waiting", "Charging", "Completed", "Released")
LOCK_TTL_SECONDS = 240
SEGMENT_MINUTES = 15
HORIZON_HOURS = 3
PAST_WINDOW_MINUTES = 45
MIN_TICK_SECONDS = 0.45
BASE_TICK_SECONDS = 3.2
QUEUE_KEY = "sim:pending_queue"
ARRIVAL_BUFFER_KEY = "sim:arrival_buffer"
TICK_COUNTER_KEY = "sim:tick_counter"
ADAPTIVE_RECOMMENDATIONS_KEY = "coordinator:adaptive_recommendations"
ADAPTIVE_COUNTER_KEY = "sim:adaptive_reallocations"
PHASE_MINUTES = 30
# Tuned for ~50–70% occupancy, staggered load, and visible queue pressure (not empty, not saturated).
PHASES = (
    {"name": "Low Traffic", "arrival_per_hour": 5.2, "retry_budget": 4, "overflow_threshold": 28},
    {"name": "Moderate Demand", "arrival_per_hour": 8.0, "retry_budget": 5, "overflow_threshold": 36},
    {"name": "Peak Congestion", "arrival_per_hour": 11.0, "retry_budget": 6, "overflow_threshold": 46},
    {"name": "Recovery Phase", "arrival_per_hour": 6.0, "retry_budget": 4, "overflow_threshold": 32},
)


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
        recommendations = self._recent_adaptive_recommendations(clock.current_time)
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
            "adaptive_recommendations": recommendations,
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
            tick_n = self._bump_tick_counter()
            self._advance_lifecycles(clock.current_time)
            phase = self._traffic_phase(clock.current_time)
            self._ingest_arrivals(clock.current_time, clock.speed, phase)
            self._drain_arrival_buffer(clock.current_time, phase, tick_n)
            self._advance_pending_queue(clock.current_time, phase)
            self._decay_coordination_phases(clock.current_time)
            self._advance_lifecycles(clock.current_time)
            self._scan_terminal_vehicle_phases(clock.current_time)
            self._emit_congestion_pressure(clock.current_time, tick_n)
            self._publish_snapshot()

    @staticmethod
    def _tick_minutes(speed: int) -> int:
        return max(3, min(18, 3 + int(speed) * 2))

    @staticmethod
    def _tick_delay(speed: int) -> float:
        return max(MIN_TICK_SECONDS, BASE_TICK_SECONDS / max(1, int(speed)))

    @staticmethod
    def _requests_per_tick(speed: int) -> int:
        base = 1 + min(2, int(speed) // 3)
        jitter = random.randint(0, 1)
        surge = random.randint(1, 2) if random.random() < min(0.2, 0.05 + int(speed) * 0.01) else 0
        return base + jitter + surge

    @staticmethod
    def _poisson_sample(mean: float) -> int:
        if mean <= 0:
            return 0
        threshold = pow(2.718281828459045, -mean)
        k = 0
        p = 1.0
        while p > threshold:
            k += 1
            p *= random.random()
        return k - 1

    def _traffic_phase(self, now: datetime) -> dict[str, Any]:
        minute_of_day = now.hour * 60 + now.minute
        idx = (minute_of_day // PHASE_MINUTES) % len(PHASES)
        phase = dict(PHASES[idx])
        phase["index"] = idx
        return phase

    @staticmethod
    def _vehicle_timing_jitter(vehicle_id: str) -> tuple[int, int]:
        """Stable plug-in delay and post-charge linger (simulated minutes)."""
        h = hash(vehicle_id) & 0xFFFFFFFF
        plug_min = 4 + (h % 6) * 2  # 4–14
        linger_min = 3 + ((h >> 6) % 6) * 2  # 3–13 after nominal end before DB completes
        return plug_min, linger_min

    def _balanced_preferred_station(self) -> int:
        raw = self._redis_get_json(TICK_COUNTER_KEY)
        n = int(raw) if raw is not None else 0
        primary = (n % len(STATION_LIMITS)) + 1
        if random.random() < 0.35:
            alts = [s for s in STATION_LIMITS if s != primary]
            return random.choice(alts) if alts else primary
        return primary

    def _bump_tick_counter(self) -> int:
        raw = self._redis_get_json(TICK_COUNTER_KEY)
        try:
            current = int(raw) if raw is not None else 0
        except (TypeError, ValueError):
            current = 0
        nxt = current + 1
        self._redis_set_json(TICK_COUNTER_KEY, nxt)
        return nxt

    def _read_arrival_buffer(self) -> list[dict[str, Any]]:
        return self._redis_get_json(ARRIVAL_BUFFER_KEY) or []

    def _write_arrival_buffer(self, buf: list[dict[str, Any]]) -> None:
        self._redis_set_json(ARRIVAL_BUFFER_KEY, buf[-220:])

    def _drain_arrival_buffer(self, now: datetime, phase: dict[str, Any], tick_n: int) -> None:
        buf = self._read_arrival_buffer()
        if not buf:
            return
        cap = random.choice([1, 2]) if phase["index"] <= 2 else random.choice([1, 2, 2])
        if tick_n % 4 == 0:
            cap = min(cap + 1, 3)
        batch = []
        remainder = []
        for item in buf:
            if len(batch) < cap:
                jitter = timedelta(minutes=random.choice([0, 0, 2, 4]))
                item["next_retry_at"] = max(
                    datetime.fromisoformat(item.get("next_retry_at", now.isoformat())),
                    now + jitter,
                ).isoformat()
                batch.append(item)
            else:
                remainder.append(item)
        for item in batch:
            self._enqueue_vehicle(item, "staggered_intake", now)
        self._write_arrival_buffer(remainder)

    @staticmethod
    def _failure_narrative(
        reason: str, vehicle_id: str, attempts: int, preferred: int | None = None
    ) -> tuple[str, str]:
        station_hint = ""
        if preferred:
            station_hint = f" (preferred Station {preferred})"
        if attempts >= 4:
            return (
                "conflict_escalating",
                f"{vehicle_id}: sustained contention after {attempts} allocation passes — backoff lengthened.",
            )
        messages = {
            "interval_collision": (
                f"{vehicle_id} blocked by overlapping reservation window — coordinator will retry{station_hint}."
            ),
            "fragmentation_failure": (
                f"{vehicle_id} waiting due to fragmented availability — no uninterrupted charging block{station_hint}."
            ),
            "station_saturation": (
                f"{vehicle_id} waiting: station saturation; allocation deferred to a later orchestration cycle."
            ),
            "no_fit": (f"{vehicle_id} deferred: coordinated window not yet available across the mesh."),
        }
        return ("conflict_detected", messages.get(reason, messages["no_fit"]))

    def _scan_terminal_vehicle_phases(self, now: datetime) -> None:
        """Complete to Released linger, then remove completed vehicles from the active roster."""
        for key in list(self._redis_keys("vehicle:*:state")):
            payload = self._redis_get_json(key)
            if not payload or not payload.get("vehicle_id"):
                continue
            status = payload.get("status")
            if status == "Completed":
                cu = payload.get("completed_until")
                if cu and now >= datetime.fromisoformat(cu):
                    merged = dict(payload)
                    merged["status"] = "Released"
                    merged.pop("completed_until", None)
                    merged["released_until"] = (
                        now + timedelta(minutes=6 + random.randint(0, 5))
                    ).isoformat()
                    merged.pop("coordination_phase", None)
                    self._redis_set_json(key, merged)
                    self._publish_event(
                        None,
                        payload["vehicle_id"],
                        "queue_updated",
                        f"{payload['vehicle_id']} session handoff complete — leaving active coordination roster.",
                    )
            elif status == "Released":
                ru = payload.get("released_until")
                if ru and now >= datetime.fromisoformat(ru):
                    self._redis_delete(key)

    def _decay_coordination_phases(self, _now: datetime) -> None:
        """Drop short-lived coordination labels after one visible tick."""
        for key in list(self._redis_keys("vehicle:*:state")):
            payload = self._redis_get_json(key)
            if not payload or payload.get("coordination_phase") != "REROUTING":
                continue
            if str(payload.get("status")) != "Reserved":
                continue
            rid = payload.get("reservation_id")
            if rid is None:
                continue
            merged = dict(payload)
            merged.pop("coordination_phase", None)
            merged["reroute_note"] = (
                f"Reroute applied; session anchored at Station {payload.get('assigned_station')}."
            )
            self._redis_set_json(key, merged)

    def _emit_congestion_pressure(self, now: datetime, tick_n: int) -> None:
        if tick_n % 3 != 0:
            return
        for station_id, charger_count in STATION_LIMITS.items():
            live = self._redis_hgetall(f"station:{station_id}:slots:live")
            occupied = sum(
                1 for cid in range(1, charger_count + 1) if live.get(str(cid), "free") != "free"
            )
            ratio = occupied / max(1, charger_count)
            if ratio < 0.68:
                continue
            self._publish_event(
                station_id,
                None,
                "station_congestion",
                f"{STATION_NAMES[station_id]} congestion threshold exceeded "
                f"({occupied}/{charger_count} bays in active orchestration).",
            )

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
            vid = str(row.get("vehicle_id") or "")
            _plug_m, linger_m = self._vehicle_timing_jitter(vid)
            nominal_end = row["reservation_end"]
            eff_end = nominal_end + timedelta(minutes=linger_m)
            display_end = nominal_end
            if str(row["reservation_status"]).lower() == "charging" and now < eff_end:
                display_end = eff_end
            interval = {
                "reservation_id": row["id"],
                "charger_id": row["charger_id"],
                "vehicle_id": row["vehicle_id"],
                "start": row["reservation_start"].isoformat(),
                "end": display_end.isoformat(),
                "status": status,
            }
            intervals_by_station.setdefault(row["station_id"], []).append(interval)
            st = row["reservation_start"]
            if status == "waiting" and st <= now:
                self._redis_hset(
                    f"station:{row['station_id']}:slots:live", str(row["charger_id"]), "waiting"
                )
            elif status in {"reserved", "charging"}:
                self._redis_hset(f"station:{row['station_id']}:slots:live", str(row["charger_id"]), status)
        for station_id, intervals in intervals_by_station.items():
            self._write_station_timeline(station_id, intervals)

    def _station_timeline_snapshot(self, now: datetime) -> list[dict[str, Any]]:
        self._hydrate_from_postgres()
        stations = []
        window_start = now - timedelta(minutes=PAST_WINDOW_MINUTES)
        window_end = window_start + timedelta(hours=HORIZON_HOURS)
        tick_count = int((window_end - window_start).total_seconds() // (30 * 60))
        ticks = [window_start + timedelta(minutes=30 * i) for i in range(0, tick_count + 1)]
        for station_id, charger_count in STATION_LIMITS.items():
            intervals = self._read_station_timeline(station_id)
            live = self._redis_hgetall(f"station:{station_id}:slots:live")
            chargers = []
            for charger_id in range(1, charger_count + 1):
                segments = self._build_segments(charger_id, window_start, intervals)
                chargers.append(
                    {
                        "charger_id": charger_id,
                        "state": live.get(str(charger_id), "free"),
                        "segments": segments,
                    }
                )
            free_windows = self._free_windows(chargers)
            adaptive_overlays = self._station_adaptive_overlays(
                station_id=station_id,
                window_start=window_start,
                window_end=window_end,
            )
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
                    "adaptive_overlays": adaptive_overlays,
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
        # Backward-compatible entrypoint used by older flows.
        self._enqueue_vehicle(self._new_vehicle_request(now), "arrival_wave", now)

    def _new_vehicle_request(self, now: datetime) -> dict[str, Any]:
        vehicle_id = f"EV-{int(time.time() * 1000)}-{random.randint(100, 999)}"
        preferred = self._balanced_preferred_station()
        emergency = random.random() < 0.07
        priority = 4 if emergency else random.choice([1, 1, 2, 2, 2, 3])
        duration = timedelta(minutes=random.choice([30, 40, 45, 55, 60, 65, 75, 85, 90]))
        earliest_offset = random.choice([5, 10, 12, 15, 18, 22, 28, 35, 40, 50])
        earliest = now + timedelta(minutes=earliest_offset)
        return {
            "vehicle_id": vehicle_id,
            "priority": priority,
            "duration_minutes": int(duration.total_seconds() / 60),
            "earliest": earliest.isoformat(),
            "soc": random.randint(18, 62),
            "attempts": 0,
            "queued_at": now.isoformat(),
            "next_retry_at": now.isoformat(),
            "emergency": emergency,
            "preferred_station": preferred,
        }

    def _ingest_arrivals(self, now: datetime, speed: int, phase: dict[str, Any]) -> None:
        tick_hours = self._tick_minutes(speed) / 60.0
        mean = float(phase["arrival_per_hour"]) * tick_hours
        arrivals = max(0, self._poisson_sample(mean))
        if arrivals <= 0:
            return
        buf = self._read_arrival_buffer()
        burst_cap = max(1, min(5, arrivals))
        arrivals = min(arrivals, burst_cap)
        for _ in range(arrivals):
            buf.append(self._new_vehicle_request(now))
        self._write_arrival_buffer(buf)
        if arrivals:
            self._publish_event(
                station_id=None,
                vehicle_id=None,
                event_type="queue_updated",
                message=(
                    f"Demand wave: +{arrivals} inbound vehicle(s) during {phase['name']} — "
                    "staggered entry into orchestration queues."
                ),
            )

    def _enqueue_vehicle(self, request: dict[str, Any], reason: str, now: datetime) -> None:
        queue = self._read_pending_queue()
        queue.append(request)
        preempted = False
        if request.get("emergency"):
            for item in queue[:-1]:
                if int(item.get("priority", 1)) <= 1 and not item.get("emergency"):
                    cur = datetime.fromisoformat(item.get("next_retry_at", now.isoformat()))
                    item["next_retry_at"] = (cur + timedelta(minutes=random.randint(6, 15))).isoformat()
                    preempted = True
                    self._publish_event(
                        station_id=None,
                        vehicle_id=item["vehicle_id"],
                        event_type="priority_preempted",
                        message=(
                            f"Emergency vehicle {request['vehicle_id']} preempted queue order; "
                            f"{item['vehicle_id']} allocation retry delayed."
                        ),
                    )
                    break
        self._write_pending_queue(queue)
        self._redis_set_json(
            f"vehicle:{request['vehicle_id']}:state",
            {
                "status": "Searching",
                "vehicle_id": request["vehicle_id"],
                "soc": request.get("soc"),
                "priority": request["priority"],
                "queue_attempts": request.get("attempts", 0),
                "queued_at": request.get("queued_at"),
            },
        )
        if request.get("emergency"):
            self._publish_event(
                station_id=None,
                vehicle_id=request["vehicle_id"],
                event_type="priority_preempted",
                message=(
                    f"{request['vehicle_id']} registered as emergency priority — expedited orchestration."
                    + ("" if preempted else " No lower-priority deferrals required this cycle.")
                ),
            )

    def _advance_pending_queue(self, now: datetime, phase: dict[str, Any]) -> None:
        queue = self._read_pending_queue()
        if not queue:
            return
        overflow = int(phase["overflow_threshold"])
        if len(queue) >= overflow:
            self._publish_event(
                station_id=None,
                vehicle_id=None,
                event_type="queue_overflow",
                message=f"Queue overflow threshold reached ({len(queue)} pending) during {phase['name']}.",
            )
        queue.sort(
            key=lambda item: (
                -int(item.get("priority", 1)),
                -int(bool(item.get("emergency"))),
                datetime.fromisoformat(item.get("queued_at", now.isoformat())),
                item.get("vehicle_id", ""),
            )
        )
        processed = 0
        survivors: list[dict[str, Any]] = []
        max_try = min(int(phase["retry_budget"]), 3)
        for item in queue:
            next_retry_at = datetime.fromisoformat(item.get("next_retry_at", now.isoformat()))
            if next_retry_at > now:
                survivors.append(item)
                continue
            processed += 1
            if processed > max_try:
                survivors.append(item)
                continue
            if not self._attempt_allocation(item, now, len(queue)):
                survivors.append(item)
        self._write_pending_queue(survivors)

    def _attempt_allocation(self, request: dict[str, Any], now: datetime, queue_size: int) -> bool:
        vehicle_id = request["vehicle_id"]
        priority = int(request["priority"])
        duration = timedelta(minutes=int(request["duration_minutes"]))
        earliest = max(now, datetime.fromisoformat(request["earliest"]))
        prev_state = self._redis_get_json(f"vehicle:{vehicle_id}:state") or {}
        alloc_payload = {
            **prev_state,
            "status": "Waiting" if int(request.get("attempts", 0)) > 0 else "Searching",
            "vehicle_id": vehicle_id,
            "soc": request.get("soc"),
            "priority": priority,
            "queue_attempts": request.get("attempts", 0),
            "queued_at": request.get("queued_at"),
            "coordination_phase": "ALLOCATING",
        }
        self._redis_set_json(f"vehicle:{vehicle_id}:state", alloc_payload)
        self._publish_event(
            station_id=None,
            vehicle_id=vehicle_id,
            event_type="allocating",
            message=f"Coordinator resolving slots for {vehicle_id} across the multi-station mesh.",
        )
        decision = self._choose_interval(
            vehicle_id=vehicle_id,
            priority=priority,
            earliest=earliest,
            duration=duration,
            queue_size=queue_size,
            preferred_station=int(request.get("preferred_station", 1)),
        )
        self._redis_set_json("coordinator:latest_decision", decision)
        allocation = decision.get("allocation")
        if not allocation:
            request["attempts"] = int(request.get("attempts", 0)) + 1
            jitter = random.randint(0, 4)
            wait_backoff = min(36, 7 + request["attempts"] * 4 + jitter)
            request["next_retry_at"] = (now + timedelta(minutes=wait_backoff)).isoformat()
            reason = decision.get("failure_reason", "no_fit")
            request["last_failure"] = reason
            evt, msg = self._failure_narrative(reason, vehicle_id, request["attempts"], request.get("preferred_station"))
            evt_out = evt
            if evt != "conflict_escalating" and request["attempts"] >= 2:
                evt_out = "allocation_retry"
            self._redis_set_json(
                f"vehicle:{vehicle_id}:state",
                {
                    "status": "Waiting",
                    "vehicle_id": vehicle_id,
                    "soc": request.get("soc"),
                    "priority": priority,
                    "queue_attempts": request["attempts"],
                    "queued_at": request.get("queued_at"),
                    "waiting_reason": reason,
                    "coordination_phase": "CONFLICT",
                },
            )
            self._publish_event(
                station_id=None,
                vehicle_id=vehicle_id,
                event_type=evt_out,
                message=msg,
            )
            return False
        ok = self._commit_allocation(request, allocation, now)
        if ok:
            adaptive_recommendation = decision.get("adaptive_recommendation")
            if adaptive_recommendation:
                self._record_adaptive_recommendation(adaptive_recommendation)
            self._publish_event(
                station_id=decision.get("selected_station"),
                vehicle_id=vehicle_id,
                event_type="coordinator_decision",
                message=decision["reasoning"],
            )
        return ok

    def _commit_allocation(self, request: dict[str, Any], allocation: dict[str, Any], now: datetime) -> bool:
        vehicle_id = request["vehicle_id"]
        priority = int(request["priority"])
        preferred = int(request.get("preferred_station", allocation["station_id"]))
        user_id = self._ensure_user(vehicle_id, priority)
        lock_key = (
            f"lock:{allocation['station_id']}:{allocation['charger_id']}:"
            f"{allocation['start'].replace(':', '').replace('+', '_')}"
        )
        if not self._redis_set(lock_key, vehicle_id, nx=True, ex=LOCK_TTL_SECONDS):
            request["attempts"] = int(request.get("attempts", 0)) + 1
            request["next_retry_at"] = (now + timedelta(minutes=6)).isoformat()
            self._publish_event(
                station_id=allocation["station_id"],
                vehicle_id=vehicle_id,
                event_type="queue_updated",
                message=f"{vehicle_id} delayed; selected slot still lock-held.",
            )
            return False
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
            request["attempts"] = int(request.get("attempts", 0)) + 1
            request["next_retry_at"] = (now + timedelta(minutes=7)).isoformat()
            self._publish_event(
                station_id=allocation["station_id"],
                vehicle_id=vehicle_id,
                event_type="conflict_detected",
                message=f"{vehicle_id} hit interval collision; retrying in next cycle.",
            )
            return False
        self._hydrate_from_postgres()
        station_name = STATION_NAMES.get(allocation["station_id"], f"Station {allocation['station_id']}")
        rerouted = preferred != allocation["station_id"]
        vehicle_payload: dict[str, Any] = {
            "status": "Reserved",
            "vehicle_id": vehicle_id,
            "assigned_station": allocation["station_id"],
            "assigned_charger": allocation["charger_id"],
            "reservation_id": reservation_id,
            "soc": request.get("soc"),
            "priority": priority,
            "queue_attempts": request.get("attempts", 0),
            "queued_at": request.get("queued_at"),
        }
        if rerouted:
            vehicle_payload["coordination_phase"] = "REROUTING"
        self._redis_set_json(f"vehicle:{vehicle_id}:state", vehicle_payload)
        if rerouted:
            self._publish_event(
                station_id=allocation["station_id"],
                vehicle_id=vehicle_id,
                event_type="rerouting",
                message=(
                    f"Coordinator rerouted {vehicle_id} from preferred Station {preferred} to "
                    f"{station_name} (charger {allocation['charger_id']}) due to congestion / fragmentation."
                ),
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
        return True

    def _choose_interval(
        self,
        vehicle_id: str,
        priority: int,
        earliest: datetime,
        duration: timedelta,
        queue_size: int = 0,
        preferred_station: int = 1,
    ) -> dict[str, Any]:
        candidate_stations = []
        best = None
        failure_reason = "no_fit"
        failure_message = ""
        for station_id, charger_count in STATION_LIMITS.items():
            intervals = self._read_station_timeline(station_id)
            allocation, station_meta = self._find_station_window(station_id, charger_count, intervals, earliest, duration)
            wait_minutes = (
                int((datetime.fromisoformat(allocation["start"]) - earliest).total_seconds() / 60)
                if allocation
                else 999
            )
            load = len(
                [item for item in intervals if item["status"] in {"reserved", "charging", "waiting"}]
            )
            reroute_cost = abs(preferred_station - station_id) * 2
            score = (
                wait_minutes
                + load * 3
                + station_meta["fragmentation_score"] * 2
                + min(16, queue_size // 2)
                + reroute_cost
                - priority * 4
            )
            candidate = {
                "station_id": station_id,
                "station_name": STATION_NAMES[station_id],
                "wait_minutes": wait_minutes,
                "active_intervals": load,
                "fragmentation_score": station_meta["fragmentation_score"],
                "congestion_score": station_meta["congestion_score"],
                "rejection_reason": station_meta["rejection_reason"],
                "score": score,
                "allocation": allocation,
            }
            candidate_stations.append(candidate)
            if allocation and (best is None or score < best["score"]):
                best = candidate
            if not allocation and station_meta["rejection_reason"]:
                failure_reason = station_meta["rejection_reason"]
                failure_message = station_meta["failure_message"]
        requested_charger = self._preferred_charger(vehicle_id, STATION_LIMITS.get(preferred_station, 1))
        requested_end = earliest + duration
        requested_intervals = self._read_station_timeline(preferred_station)
        requested_overlap = self._interval_overlaps(
            requested_intervals,
            requested_charger,
            earliest,
            requested_end,
        ) or self._db_conflict(preferred_station, requested_charger, earliest, requested_end)
        adaptive_recommendation = None
        if requested_overlap and best and best.get("allocation"):
            adaptive_recommendation = self._build_adaptive_recommendation(
                vehicle_id=vehicle_id,
                priority=priority,
                requested_station=preferred_station,
                requested_charger=requested_charger,
                requested_start=earliest,
                requested_end=requested_end,
                suggested=best["allocation"],
                score=int(best["score"]),
            )
        reasoning = (
            f"{vehicle_id} assigned to {STATION_NAMES[best['station_id']]} using lowest congestion/fragmentation score "
            f"({best['score']}) with wait {best['wait_minutes']} min."
            if best
            else f"{vehicle_id} remains queued: {failure_message or 'continuous charging interval unavailable.'}"
        )
        return {
            "vehicle_id": vehicle_id,
            "priority": priority,
            "candidate_stations": candidate_stations,
            "selected_station": best["station_id"] if best else None,
            "allocation": best["allocation"] if best else None,
            "adaptive_recommendation": adaptive_recommendation,
            "reasoning": reasoning,
            "failure_reason": failure_reason,
            "failure_message": failure_message,
            "decided_at": datetime.now(timezone.utc).isoformat(),
        }

    def _find_station_window(
        self,
        station_id: int,
        charger_count: int,
        intervals: list[dict[str, Any]],
        earliest: datetime,
        duration: timedelta,
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        free_windows = 0
        fragmented_windows = 0
        active_count = 0
        horizon_end = earliest + timedelta(hours=HORIZON_HOURS)
        for charger_id in range(1, charger_count + 1):
            cursor = earliest
            for offset in range(0, HORIZON_HOURS * 60, SEGMENT_MINUTES):
                start = earliest + timedelta(minutes=offset)
                end = start + timedelta(minutes=SEGMENT_MINUTES)
                occupied = any(
                    int(item["charger_id"]) == charger_id
                    and datetime.fromisoformat(item["start"]) < end
                    and datetime.fromisoformat(item["end"]) > start
                    and item["status"] in {"reserved", "waiting", "charging"}
                    for item in intervals
                )
                if occupied:
                    active_count += 1
                    if cursor < start:
                        window = start - cursor
                        free_windows += 1
                        if window < duration:
                            fragmented_windows += 1
                    cursor = end
            if cursor < horizon_end:
                window = horizon_end - cursor
                free_windows += 1
                if window < duration:
                    fragmented_windows += 1
        for offset in range(0, HORIZON_HOURS * 60, SEGMENT_MINUTES):
            start = earliest + timedelta(minutes=offset)
            end = start + duration
            if end > earliest + timedelta(hours=HORIZON_HOURS):
                break
            for charger_id in range(1, charger_count + 1):
                has_conflict = any(
                    int(item["charger_id"]) == charger_id
                    and datetime.fromisoformat(item["start"]) < end
                    and datetime.fromisoformat(item["end"]) > start
                    and item["status"] in {"reserved", "charging", "waiting"}
                    for item in intervals
                )
                if not has_conflict and not self._db_conflict(station_id, charger_id, start, end):
                    return (
                        {
                            "station_id": station_id,
                            "charger_id": charger_id,
                            "start": start.isoformat(),
                            "end": end.isoformat(),
                        },
                        {
                            "fragmentation_score": fragmented_windows,
                            "congestion_score": active_count,
                            "rejection_reason": "",
                            "failure_message": "",
                        },
                    )
        rejection_reason = "station_saturation"
        failure_message = f"Station {station_id} saturated; no uninterrupted window."
        if free_windows and fragmented_windows >= max(1, free_windows // 2):
            rejection_reason = "fragmentation_failure"
            failure_message = "Free slots exist but no continuous charging interval is available."
        return (
            None,
            {
                "fragmentation_score": fragmented_windows,
                "congestion_score": active_count,
                "rejection_reason": rejection_reason,
                "failure_message": failure_message,
            },
        )

    @staticmethod
    def _preferred_charger(vehicle_id: str, charger_count: int) -> int:
        if charger_count <= 1:
            return 1
        return (sum(ord(ch) for ch in vehicle_id) % charger_count) + 1

    @staticmethod
    def _interval_overlaps(intervals: list[dict[str, Any]], charger_id: int, start: datetime, end: datetime) -> bool:
        return any(
            int(item["charger_id"]) == charger_id
            and datetime.fromisoformat(item["start"]) < end
            and datetime.fromisoformat(item["end"]) > start
            and item["status"] in {"reserved", "waiting", "charging"}
            for item in intervals
        )

    def _build_adaptive_recommendation(
        self,
        *,
        vehicle_id: str,
        priority: int,
        requested_station: int,
        requested_charger: int,
        requested_start: datetime,
        requested_end: datetime,
        suggested: dict[str, Any],
        score: int,
    ) -> dict[str, Any]:
        suggested_start = datetime.fromisoformat(suggested["start"])
        delay_minutes = max(0, int((suggested_start - requested_start).total_seconds() / 60))
        optimization_score = -max(1, min(18, abs(score) // 4 + delay_minutes // 15 + (5 - min(priority, 5))))
        return {
            "id": f"{vehicle_id}:{int(requested_start.timestamp())}:{suggested['station_id']}:{suggested['charger_id']}",
            "vehicle_id": vehicle_id,
            "title": "Adaptive Scheduling",
            "summary": "Preferred interval unavailable",
            "requested": {
                "station_id": requested_station,
                "station_name": STATION_NAMES.get(requested_station, f"Station {requested_station}"),
                "charger_id": requested_charger,
                "start": requested_start.isoformat(),
                "end": requested_end.isoformat(),
            },
            "suggested": {
                "station_id": int(suggested["station_id"]),
                "station_name": STATION_NAMES.get(int(suggested["station_id"]), f"Station {suggested['station_id']}"),
                "charger_id": int(suggested["charger_id"]),
                "start": suggested["start"],
                "end": suggested["end"],
            },
            "estimated_delay_minutes": delay_minutes,
            "optimization_score": optimization_score,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def _record_adaptive_recommendation(self, recommendation: dict[str, Any]) -> None:
        recent = self._redis_get_json(ADAPTIVE_RECOMMENDATIONS_KEY) or []
        if any(item.get("id") == recommendation.get("id") for item in recent):
            return
        recent.insert(0, recommendation)
        self._redis_set_json(ADAPTIVE_RECOMMENDATIONS_KEY, recent[:12])
        current = self._redis_get_json(ADAPTIVE_COUNTER_KEY) or 0
        try:
            current = int(current)
        except (TypeError, ValueError):
            current = 0
        self._redis_set_json(ADAPTIVE_COUNTER_KEY, current + 1)
        requested = recommendation["requested"]
        suggested = recommendation["suggested"]
        start_label = self._display_time(datetime.fromisoformat(suggested["start"]))
        end_label = self._display_time(datetime.fromisoformat(suggested["end"]))
        self._publish_event(
            station_id=int(suggested["station_id"]),
            vehicle_id=recommendation.get("vehicle_id"),
            event_type="adaptive_allocation",
            message=(
                f"Requested interval at {requested['station_name']} C{requested['charger_id']} unavailable. "
                f"Suggested nearest available window: C{suggested['charger_id']} {start_label} - {end_label}."
            ),
        )

    def _recent_adaptive_recommendations(self, now: datetime) -> list[dict[str, Any]]:
        del now
        recent = self._redis_get_json(ADAPTIVE_RECOMMENDATIONS_KEY) or []
        fresh = []
        wall_now = datetime.now(timezone.utc)
        for item in recent:
            created_at = item.get("created_at")
            if not created_at:
                continue
            try:
                age = wall_now - datetime.fromisoformat(created_at)
            except ValueError:
                continue
            if age <= timedelta(minutes=45):
                fresh.append(item)
        if len(fresh) != len(recent):
            self._redis_set_json(ADAPTIVE_RECOMMENDATIONS_KEY, fresh[:12])
        return fresh[:6]

    def _station_adaptive_overlays(
        self,
        *,
        station_id: int,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, Any]]:
        overlays = []
        for item in self._recent_adaptive_recommendations(self._read_clock().current_time):
            for kind in ("requested", "suggested"):
                slot = item.get(kind) or {}
                if int(slot.get("station_id", -1)) != station_id:
                    continue
                start = datetime.fromisoformat(slot["start"])
                end = datetime.fromisoformat(slot["end"])
                if start < window_end and end > window_start:
                    overlays.append(
                        {
                            "id": item.get("id"),
                            "type": kind,
                            "charger_id": int(slot["charger_id"]),
                            "start": slot["start"],
                            "end": slot["end"],
                            "vehicle_id": item.get("vehicle_id"),
                        }
                    )
        return overlays

    @staticmethod
    def _display_time(value: datetime) -> str:
        if value.tzinfo is not None:
            value = value.astimezone()
        return value.strftime("%I:%M %p").lstrip("0").lower()


    def _advance_lifecycles(self, now: datetime) -> None:
        rows = self._reservation_rows(now - timedelta(hours=2), now + timedelta(hours=HORIZON_HOURS))
        for row in rows:
            vehicle_id = row["vehicle_id"]
            rs = str(row["reservation_status"]).lower()
            if rs == "completed":
                continue
            st = row["reservation_start"]
            en = row["reservation_end"]
            plug_m, linger_m = self._vehicle_timing_jitter(vehicle_id)
            plug_end = st + timedelta(minutes=plug_m)
            eff_end = en + timedelta(minutes=linger_m)

            if now >= eff_end and rs != "completed":
                self._update_reservation_status(row["id"], "completed")
                self._complete_session(row, now)
                self._redis_hset(f"station:{row['station_id']}:slots:live", str(row["charger_id"]), "free")
                dwell = timedelta(minutes=8 + random.randint(0, 6))
                self._redis_set_json(
                    f"vehicle:{vehicle_id}:state",
                    {
                        "status": "Completed",
                        "vehicle_id": vehicle_id,
                        "assigned_station": row["station_id"],
                        "assigned_charger": row["charger_id"],
                        "reservation_id": row["id"],
                        "completed_until": (now + dwell).isoformat(),
                    },
                )
                logger.info(
                    "Charging lifecycle transition: reservation_id=%s vehicle_id=%s status=completed",
                    row["id"],
                    vehicle_id,
                )
                self._publish_event(
                    row["station_id"],
                    vehicle_id,
                    "charging_completed",
                    f"{vehicle_id} tapering charge complete after sustained session — bay releasing shortly.",
                )
                self._publish_event(
                    row["station_id"],
                    vehicle_id,
                    "slot_released",
                    f"Charger {row['charger_id']} released following session wrap-up.",
                )
                continue

            if plug_end <= now < eff_end and rs != "charging":
                self._update_reservation_status(row["id"], "charging")
                self._start_session(row, now)
                self._redis_hset(f"station:{row['station_id']}:slots:live", str(row["charger_id"]), "charging")
                self._redis_set_json(
                    f"vehicle:{vehicle_id}:state",
                    {
                        "status": "Charging",
                        "vehicle_id": vehicle_id,
                        "assigned_station": row["station_id"],
                        "assigned_charger": row["charger_id"],
                        "reservation_id": row["id"],
                    },
                )
                logger.info(
                    "Charging lifecycle transition: reservation_id=%s vehicle_id=%s status=charging",
                    row["id"],
                    vehicle_id,
                )
                self._publish_event(
                    row["station_id"],
                    vehicle_id,
                    "charging_started",
                    f"{vehicle_id} began power delivery after plug-in staging at Station {row['station_id']}.",
                )
                continue

            if st <= now < plug_end:
                if rs not in {"waiting"}:
                    self._update_reservation_status(row["id"], "waiting")
                    self._publish_event(
                        row["station_id"],
                        vehicle_id,
                        "vehicle_waiting",
                        f"{vehicle_id} staged at charger — plug-in handshake before energy transfer.",
                    )
                self._redis_set_json(
                    f"vehicle:{vehicle_id}:state",
                    {
                        "status": "Waiting",
                        "vehicle_id": vehicle_id,
                        "assigned_station": row["station_id"],
                        "assigned_charger": row["charger_id"],
                        "reservation_id": row["id"],
                        "waiting_reason": "plug_in_handshake",
                    },
                )
                continue

            if st - timedelta(minutes=15) <= now < st and rs == "reserved":
                self._update_reservation_status(row["id"], "waiting")
                self._set_vehicle_status(vehicle_id, "Waiting", row)
                logger.info(
                    "Charging lifecycle transition: reservation_id=%s vehicle_id=%s status=waiting",
                    row["id"],
                    vehicle_id,
                )
                self._publish_event(
                    row["station_id"],
                    vehicle_id,
                    "vehicle_waiting",
                    f"{vehicle_id} arriving within coordination pre-window for reserved interval.",
                )

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
                if charger["state"] in {"reserved", "waiting"}:
                    pending += 1
        metrics = {
            "active_vehicles": len([v for v in vehicles if v.get("status") not in {"Completed", "Released"}]),
            "active_sessions": active_sessions,
            "pending_reservations": pending,
            "free_windows": free_windows,
            "adaptive_reallocations": int(self._redis_get_json(ADAPTIVE_COUNTER_KEY) or 0),
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
        rs = str(row["reservation_status"]).lower()
        if rs == "completed":
            return "completed"
        vid = str(row.get("vehicle_id") or "")
        st = row["reservation_start"]
        en = row["reservation_end"]
        plug_m, linger_m = OrchestrationService._vehicle_timing_jitter(vid)
        plug_end = st + timedelta(minutes=plug_m)
        eff_end = en + timedelta(minutes=linger_m)
        if now >= eff_end:
            return "completed"
        if plug_end <= now < eff_end:
            return "charging"
        if st <= now < plug_end:
            return "waiting"
        if st - timedelta(minutes=15) <= now < st:
            return "waiting"
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

    def _read_pending_queue(self) -> list[dict[str, Any]]:
        return self._redis_get_json(QUEUE_KEY) or []

    def _write_pending_queue(self, queue: list[dict[str, Any]]) -> None:
        self._redis_set_json(QUEUE_KEY, queue[:180])

    def _clear_runtime_keys(self) -> None:
        for pattern in (
            "station:*:timeline",
            "station:*:slots:live",
            "vehicle:*:state",
            "lock:*",
            "events:stream",
            "sim:*",
            "coordinator:*",
        ):
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
