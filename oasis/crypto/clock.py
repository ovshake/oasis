"""Fixed-interval tick clock for the crypto exchange simulation.

Phase 8 deliverable. Maps simulation steps to wall-clock datetimes.
Step N -> start_datetime + N * step_minutes * 60 seconds.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pydantic import BaseModel


class TickClock(BaseModel):
    """Fixed-interval clock. step N -> start_datetime + N * step_minutes * 60s."""

    start_datetime: datetime  # UTC; scenario anchor
    step_minutes: int = 1
    current_step: int = 0

    model_config = {"arbitrary_types_allowed": True}

    def advance(self, n: int = 1) -> None:
        """Advance the clock by *n* steps."""
        self.current_step += n

    def reset(self) -> None:
        """Reset clock to step 0."""
        self.current_step = 0

    def step_to_datetime(self, step: int) -> datetime:
        """Convert a step number to a UTC datetime."""
        return self.start_datetime + timedelta(minutes=step * self.step_minutes)

    def datetime_to_step(self, dt: datetime) -> int:
        """Convert a UTC datetime to a step number (floor division)."""
        delta = (dt - self.start_datetime).total_seconds()
        return int(delta // (self.step_minutes * 60))

    def current_datetime(self) -> datetime:
        """Return the datetime corresponding to current_step."""
        return self.step_to_datetime(self.current_step)
