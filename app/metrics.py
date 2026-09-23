"""Bounded, concurrency-safe per-entry latency/call aggregation (local only).

No external collector is used: aggregates live in process memory behind a
single lock, so concurrent writers always see a consistent state.

Bounded memory
--------------
* at most ``max_entries`` distinct entry names are tracked (new ones beyond
  the cap are dropped and counted in ``dropped_entries``);
* each entry keeps only the last ``RECENT_SAMPLES`` raw samples;
* the latency distribution is a fixed-size histogram, never a growing list.

Counting rule
-------------
``record`` is called exactly once per *logical* entry request. A downstream
invocation that is retried three times or degrades to a fallback still enters
the entry counter once; its retry attempts and fallback usage are reported in
dedicated fields, not as extra calls.
"""
from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Deque

# Fixed latency buckets in milliseconds: (0,5],(5,10],...,(10000,+inf).
BUCKET_BOUNDS_MS: tuple[int, ...] = (
    5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000
)
HISTOGRAM_SIZE = len(BUCKET_BOUNDS_MS) + 1
DEFAULT_MAX_ENTRIES = 128
RECENT_SAMPLES = 64


def bucket_labels() -> list[str]:
    labels: list[str] = []
    prev = 0
    for bound in BUCKET_BOUNDS_MS:
        labels.append(f"{prev}-{bound}ms")
        prev = bound
    labels.append(f"{prev}ms+")
    return labels


def bucket_for(duration_ms: float) -> int:
    """Index of the histogram bucket containing ``duration_ms``."""
    for index, bound in enumerate(BUCKET_BOUNDS_MS):
        if duration_ms <= bound:
            return index
    return HISTOGRAM_SIZE - 1


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

    @property
    def avg_ms(self) -> float:
        return self.total_ms / self.count if self.count else 0.0

    def snapshot(self) -> dict:
        return {
            "entry": self.entry,
            "count": self.count,
            "error_count": self.error_count,
            "success_count": self.count - self.error_count,
            "avg_ms": round(self.avg_ms, 3),
            "min_ms": round(self.min_ms, 3) if self.min_ms is not None else None,
            "max_ms": round(self.max_ms, 3) if self.max_ms is not None else None,
            "histogram": {
                "bounds_ms": list(BUCKET_BOUNDS_MS),
                "counts": list(self.histogram),
            },
            "by_category": dict(sorted(self.by_category.items())),
            "retry_attempts": self.retry_attempts,
            "fallback_count": self.fallback_count,
            "recent": list(self.recent),
        }


class MetricsRegistry:
    def __init__(self, max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
        self._lock = threading.Lock()
        self._stats: dict[str, EntryStats] = {}
        self._max_entries = max_entries
        self._dropped_entries = 0

    def record(
        self,
        entry: str,
        duration_ms: float,
        *,
        error_category: str | None = None,
        retry_attempts: int = 0,
        fell_back: bool = False,
    ) -> None:
        """Record one logical entry request (safe under concurrency)."""
        duration_ms = max(0.0, float(duration_ms))
        with self._lock:
            stats = self._stats.get(entry)
            if stats is None:
                if len(self._stats) >= self._max_entries:
                    self._dropped_entries += 1
                    return
                stats = EntryStats(entry=entry)
                self._stats[entry] = stats

            stats.count += 1
            stats.total_ms += duration_ms
            stats.min_ms = duration_ms if stats.min_ms is None else min(stats.min_ms, duration_ms)
            stats.max_ms = duration_ms if stats.max_ms is None else max(stats.max_ms, duration_ms)
            stats.histogram[bucket_for(duration_ms)] += 1
            if error_category is not None:
                stats.error_count += 1
                stats.by_category[error_category] = (
                    stats.by_category.get(error_category, 0) + 1
                )
            if retry_attempts > 0:
                # Number of *extra* physical attempts across logical calls.
                stats.retry_attempts += retry_attempts
            if fell_back:
                stats.fallback_count += 1
            stats.recent.append(
                {
                    "duration_ms": round(duration_ms, 3),
                    "error_category": error_category,
                    "retry_attempts": retry_attempts,
                    "fell_back": fell_back,
                }
            )

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "max_entries": self._max_entries,
                "tracked_entries": len(self._stats),
                "dropped_entries": self._dropped_entries,
                "entries": {
                    name: stats.snapshot()
                    for name, stats in sorted(self._stats.items())
                },
            }

    def reset(self) -> None:
        with self._lock:
            self._stats.clear()
            self._dropped_entries = 0


registry = MetricsRegistry()
