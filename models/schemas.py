from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _require_timezone(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value


class Location(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)


class RecommendationRequest(BaseModel):
    vehicle_id: int = Field(..., gt=0)
    required_charge_kwh: float = Field(..., gt=0)
    location: Location
    requested_start: datetime | None = None

    @field_validator("requested_start")
    @classmethod
    def validate_requested_start(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return _require_timezone(value)

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "vehicle_id": 101,
                "required_charge_kwh": 20.5,
                "location": {"lat": 12.9716, "lon": 77.5946},
            }
        }
    )


class RecommendationItem(BaseModel):
    station_id: int
    point_id: int
    start_time: datetime
    end_time: datetime


class RecommendationResponse(BaseModel):
    recommendations: list[RecommendationItem] | None = None
    status: str | None = None


class ReservationRequest(BaseModel):
    vehicle_id: int = Field(..., gt=0)
    point_id: int = Field(..., gt=0)
    start_time: datetime
    end_time: datetime

    @field_validator("start_time")
    @classmethod
    def validate_start_time(cls, start_time: datetime) -> datetime:
        return _require_timezone(start_time)

    @field_validator("end_time")
    @classmethod
    def validate_time_window(cls, end_time: datetime, info):
        _require_timezone(end_time)
        start_time = info.data.get("start_time")
        if start_time is not None and end_time <= start_time:
            raise ValueError("end_time must be after start_time")
        return end_time

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "vehicle_id": 101,
                "point_id": 1,
                "start_time": "2026-04-24T10:00:00+05:30",
                "end_time": "2026-04-24T11:00:00+05:30",
            }
        }
    )


class ReservationResponse(BaseModel):
    status: str
    reservation_id: int | None = None


class SimulationStartRequest(BaseModel):
    iterations: int = Field(default=12, ge=1, le=200)


class SimulationStartResponse(BaseModel):
    started: bool
    message: str


class OccupySlotRequest(BaseModel):
    point_id: int = Field(..., gt=0)
    time_slot: str


class OccupySlotResponse(BaseModel):
    success: bool
    created: bool
    point_id: int
    time_slot: str
    reservation_id: int | None = None


class SlotToggleRequest(BaseModel):
    station_id: int = Field(..., gt=0)
    point_id: int = Field(..., gt=0)
    time: str


class SlotToggleResponse(BaseModel):
    station_id: int
    point_id: int
    time: str
    status: str


class ManualEventRequest(BaseModel):
    event_type: str
    vehicle_id: int | None = Field(default=None, gt=0)
    reservation_id: int | None = Field(default=None, gt=0)
    station_id: int | None = Field(default=None, gt=0)
    point_id: int | None = Field(default=None, gt=0)
    delay_minutes: int | None = Field(default=None, ge=1, le=240)
    required_charge_kwh: float = Field(default=20, gt=0)
    connector_type: str = "Type2"


class ManualEventResponse(BaseModel):
    status: str
    message: str
    reservation_id: int | None = None
