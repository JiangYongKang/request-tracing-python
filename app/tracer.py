"""Tracing facade: scoped spans, retry/fallback accounting, trace store."""
from __future__ import annotations

import threading
from collections import deque
from contextlib import contextmanager
from typing import Any, Deque, Iterator
from typing import Callable

from .context import (
    Span,
    TraceContext,
    current_context,
    finish_span,
    start_span,
    start_trace,
)
from .errors import ClassifiedError, classify

DEFAULT_MAX_TRACES = 512


class TraceStore:
    """Bounded in-memory store of finished traces (newest retained)."""

    def __init__(self, max_traces: int = DEFAULT_MAX_TRACES) -> None:
        self._lock = threading.Lock()
        self._traces: Deque[dict[str, Any]] = deque()
        self._max = max_traces

    def save(self, record: dict[str, Any]) -> None:
        raise NotImplementedError

    def all(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get(self, trace_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def reset(self) -> None:
        raise NotImplementedError


store = TraceStore()


def build_trace_record(ctx: TraceContext, error_category: str | None) -> dict[str, Any]:
    raise NotImplementedError


@contextmanager
def trace_scope(entry: str) -> Iterator[TraceContext]:
    raise NotImplementedError


@contextmanager
def span_scope(
    name: str, *, attrs: dict[str, Any] | None = None
) -> Iterator[Span]:
    raise NotImplementedError


@contextmanager
def logical_call(name: str) -> Iterator[Span]:
    """One logical downstream call: retries/fallback count once."""
    raise NotImplementedError


def call_with_retry(
    fn: Callable[[], Any],
    name: str,
    *,
    attempts: int = 1,
    fallback: Callable[[Exception], Any] | None = None,
) -> Any:
    raise NotImplementedError


async def call_with_retry_async(
    fn: Callable[[], Any],
    name: str,
    *,
    attempts: int = 1,
    fallback: Callable[[Exception], Any] | None = None,
) -> Any:
    raise NotImplementedError
