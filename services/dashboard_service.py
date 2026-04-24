from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime

from core.redis import redis_client
from services.station_service import load_stations_into_redis, reset_station_cache, snapshot_stations


@dataclass
class FeedItem:
    vehicle_id: int
    station_id: int
    start_time: str
    end_time: str
    success: bool
    message: str
    slot_id: int | None
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
        vehicle_id: int,
        station_id: int,
        start_time: str,
        end_time: str,
        success: bool,
        message: str,
        slot_id: int | None,
    ) -> None:
        with self._lock:
            self._stats["totalRequests"] += 1
            if success:
                self._stats["success"] += 1
            else:
                self._stats["failed"] += 1

            self._requests.appendleft(
                FeedItem(
                    vehicle_id=vehicle_id,
                    station_id=station_id,
                    start_time=start_time,
                    end_time=end_time,
                    success=success,
                    message=message,
                    slot_id=slot_id,
                    timestamp=datetime.now().isoformat(),
                )
            )

    def set_running(self, running: bool) -> None:
        with self._lock:
            self._simulation_running = running

    def is_running(self) -> bool:
        with self._lock:
            return self._simulation_running

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            requests = [item.__dict__ for item in self._requests]
            logs = [item.__dict__ for item in self._logs]
            stats = dict(self._stats)
            simulation_running = self._simulation_running

        return {
            "stations": snapshot_stations(),
            "requests": requests,
            "logs": logs,
            "stats": stats,
            "simulation": {
                "running": simulation_running,
                "redis_connected": bool(redis_client.ping()),
            },
        }


dashboard_state = DashboardState()
