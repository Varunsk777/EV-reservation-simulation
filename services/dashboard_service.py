from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime

from core.redis import redis_client
from services.station_service import load_stations_into_redis, reset_station_cache, snapshot_stations
from redis.exceptions import RedisError


@dataclass
class FeedItem:
    station_id: int
    success: bool
    message: str
    status: str
    slot_id: int | None
    point_id: int | None
    slot_time: str | None
    timestamp: str


@dataclass
class LogItem:
    timestamp: str
    message: str


class DashboardState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests: deque[FeedItem] = deque(maxlen=50)
        self._logs: deque[LogItem] = deque(maxlen=150)
        self._stats = {"totalRequests": 0, "success": 0, "failed": 0}
        self._simulation_running = False

    def initialize(self) -> None:
        reset_station_cache()
        load_stations_into_redis()
        self.add_log("Station cache loaded.")

    def reset_run(self) -> None:
        with self._lock:
            self._requests.clear()
            self._logs.clear()
            self._stats = {"totalRequests": 0, "success": 0, "failed": 0}

    def add_log(self, message: str) -> None:
        with self._lock:
            self._logs.appendleft(
                LogItem(
                    timestamp=datetime.now().isoformat(),
                    message=message,
                )
            )

    def record_request(
        self,
        *,
        station_id: int,
        success: bool,
        message: str,
        status: str | None = None,
        slot_id: int | None,
        point_id: int | None = None,
        slot_time: str | None = None,
    ) -> None:
        status = status or ("OCCUPIED" if success else "NO_AVAILABILITY")
        with self._lock:
            self._stats["totalRequests"] += 1
            if success:
                self._stats["success"] += 1
            else:
                self._stats["failed"] += 1

            self._requests.appendleft(
                FeedItem(
                    station_id=station_id,
                    success=success,
                    message=message,
                    status=status,
                    slot_id=slot_id,
                    point_id=point_id or slot_id,
                    slot_time=slot_time,
                    timestamp=datetime.now().isoformat(),
                )
            )

    def set_running(self, running: bool) -> None:
        with self._lock:
            self._simulation_running = running

    def is_running(self) -> bool:
        with self._lock:
            return self._simulation_running

    def activity_snapshot(self) -> list[dict[str, object]]:
        with self._lock:
            return [
                {
                    "type": "SIMULATION",
                    "station_id": item.station_id,
                    "point_id": item.point_id,
                    "slot_time": item.slot_time,
                    "status": item.status,
                    "timestamp": item.timestamp,
                    "message": item.message,
                }
                for item in self._requests
            ]

    def runtime_stats(self) -> dict[str, int]:
        with self._lock:
            return dict(self._stats)

    def snapshot(self) -> dict[str, object]:
        from services.observability_service import get_metrics

        with self._lock:
            requests = [item.__dict__ for item in self._requests]
            logs = [item.__dict__ for item in self._logs]
            stats = dict(self._stats)
            simulation_running = self._simulation_running

        try:
            redis_connected = bool(redis_client.ping())
        except RedisError:
            redis_connected = False

        metrics = get_metrics()
        stats["totalRequests"] = metrics["total_requests"]
        stats["success"] = metrics["success"]
        stats["failed"] = metrics["failed"]
        stats["avgWaitTime"] = metrics["avg_wait_time"]
        stats["utilization"] = metrics["utilization"]

        return {
            "stations": snapshot_stations(),
            "requests": requests,
            "logs": logs,
            "stats": stats,
            "simulation": {
                "running": simulation_running,
                "redis_connected": redis_connected,
            },
        }


dashboard_state = DashboardState()
