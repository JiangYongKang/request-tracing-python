"""Bounded, concurrency-safe per-entry latency/call aggregation (local only)."""
from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Deque

# Fixed latency buckets in milliseconds: upper bounds + inf.
BUCKET_BOUNDS_MS: tuple[int, ...] = (5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000)
HISTOGRAM_SIZE = len(BUCKET_BOUNDS_MS) + 1
DEFAULT_MAX_ENTRIES = 128
RECENT_SAMPLES = 64


@dataclass
class EntryStats:
    entry: str
    count: int = 0
    error_count: int = 0
    total_ms: float = 0.0
    min_ms: float | None = None
    max_ms: float | None = None
    histogram: list[int] = field(default_factory=lambda: [0] * HISTOGRAM_SIZE)
    by_category: dict[str, int] = field(default_factory=dict)
    retry_attempts: int = 0
    fallback_count: int = 0
    recent: Deque[dict] = field(default_factory=lambda: deque(maxlen=RECENT_SAMPLES))

    def snapshot(self) -> dict:
        raise NotImplementedError


class MetricsRegistry:
    def __init__(self, max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
        self._lock = threading.Lock()
        self._stats: dict[str, EntryStats] = {}
        self._max_entries = max_entries

    def record(
        self,
        entry: str,
        duration_ms: float,
        *,
        error_category: str | None = None,
        retry_attempts: int = 0,
        fell_back: bool = False,
    ) -> None:
        raise NotImplementedError

    def snapshot(self) -> dict[str, dict]:
        raise NotImplementedError

    def reset(self) -> None:
        raise NotImplementedError


def bucket_for(duration_ms: float) -> int:
    raise NotImplementedError


registry = MetricsRegistry()
