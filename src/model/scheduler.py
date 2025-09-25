from dataclasses import dataclass, field
from typing import Deque, Optional
from datetime import datetime
from collections import deque

@dataclass
class JobRunRecord:
    """Compact representation of a single job event."""

    event: str
    recorded_at: datetime
    scheduled_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    message: Optional[str] = None


@dataclass
class JobStats:
    """In-memory snapshot of repeated job execution metrics."""

    total_runs: int = 0
    total_success: int = 0
    total_error: int = 0
    total_missed: int = 0
    last_event: Optional[str] = None
    last_scheduled_at: Optional[datetime] = None
    last_started_at: Optional[datetime] = None
    last_finished_at: Optional[datetime] = None
    last_duration_ms: Optional[float] = None
    last_error: Optional[str] = None
    history: Deque[JobRunRecord] = field(default_factory=deque)
