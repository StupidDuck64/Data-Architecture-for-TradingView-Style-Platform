"""Pydantic models for the health check endpoint."""

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    checks: dict[str, str]
    latency_ms: dict[str, float]
    total_latency_ms: float
    checked_at: str
    uptime_sec: int
