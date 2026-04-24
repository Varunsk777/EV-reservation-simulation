from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ReservationRequest(BaseModel):
    vehicle_id: int = Field(..., gt=0)
    station_id: int = Field(..., gt=0)
    start_time: datetime
    end_time: datetime

    @field_validator("end_time")
    @classmethod
    def validate_time_window(cls, end_time: datetime, info):
        start_time = info.data.get("start_time")
        if start_time is not None and end_time <= start_time:
            raise ValueError("end_time must be after start_time")
        return end_time

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "vehicle_id": 101,
                "station_id": 1,
                "start_time": "2026-04-24T10:00:00",
                "end_time": "2026-04-24T11:00:00",
            }
        }
    )


class ReservationResponse(BaseModel):
    success: bool
    message: str
    slot_id: int | None = None


class SimulationStartRequest(BaseModel):
    iterations: int = Field(default=12, ge=1, le=200)


class SimulationStartResponse(BaseModel):
    started: bool
    message: str
