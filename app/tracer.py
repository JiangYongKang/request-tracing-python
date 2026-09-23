"""Tracing facade: scoped managers, retry/fallback accounting, trace store.

Span shape
----------
* Every entry request opens a trace whose **root span** measures the whole
  request. ``set_entry_name`` renames it once the route template is known.
* :func:`logical_call` opens a parent span for one *logical* downstream call;
  physical attempts are nested ``<name>/attempt-1..n`` child spans and the
  fallback (if used) is a ``<name>/fallback`` child. Whether the call succeeds
  on the first try or after retries, there is exactly one logical span for it.
* Metrics are recorded once per trace; retries/fallbacks do not inflate the
  per-entry call count.
"""
from __future__ import annotations

import inspect
import threading
from collections import deque
from contextlib import contextmanager
from typing import Any, Callable, Deque, Iterator

from .context import (
    Span,
    TraceContext,
    current_context,
    finish_span,
    start_span,
    start_trace,
)
from .errors import classify

DEFAULT_MAX_TRACES = 512


def _error_fields(err: BaseException) -> dict[str, Any]:
    classified = classify(err)
    return {
        "error_category": classified.category.value,
        "error_message": classified.message,
        "cause": classified.cause,
    }


class TraceStore:
    """Bounded in-memory store of finished traces (newest ``max_traces``)."""

    def __init__(self, max_traces: int = DEFAULT_MAX_TRACES) -> None:
        self._lock = threading.Lock()
        self._traces: Deque[dict[str, Any]] = deque()
        self._max = max_traces
        self._dropped = 0

    def save(self, record: dict[str, Any]) -> None:
        with self._lock:
            self._traces.append(record)
            if len(self._traces) > self._max:
                self._traces.popleft()
                self._dropped += 1

    def all(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._traces)

    def get(self, trace_id: str) -> dict[str, Any] | None:
        with self._lock:
            for record in reversed(self._traces):
                if record["trace_id"] == trace_id:
                    return record
        return None

    def reset(self) -> None:
        with self._lock:
            self._traces.clear()
            self._dropped = 0

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "stored": len(self._traces),
                "max_traces": self._max,
                "dropped": self._dropped,
            }


store = TraceStore()


def build_trace_record(ctx: TraceContext, err: BaseException | None) -> dict[str, Any]:
    """Assemble the persisted trace record. The root span receives the entry
    outcome; the original cause chain is preserved on each error span."""
    root = ctx.find_span(ctx.root_span_id)
    category: str | None = None
    if err is not None:
        fields = _error_fields(err)
        category = fields["error_category"]
        if root is not None:
            finish_span(
                root,
                error_category=category,
                error_message=fields["error_message"],
                cause=fields["cause"],
            )
    elif root is not None and root.end_perf is None:
        finish_span(root)

    record = ctx.snapshot()
    record["status"] = "error" if category else "ok"
    record["error_category"] = category
    if root is not None:
        record["duration_ms"] = root.duration_ms
    record["retry_attempts"] = getattr(ctx, "retry_attempts", 0)
    record["fell_back"] = getattr(ctx, "fell_back", False)
    return record


@contextmanager
def trace_scope(entry: str) -> Iterator[TraceContext]:
    """Context-managed trace: root span open for the whole request.

    Retry/fallback accumulators are attached to the trace object by
    :func:`call_with_retry` / :func:`call_with_retry_async`.
    """
    ctx, deactivate = start_trace(entry)
    ctx.retry_attempts = 0  # type: ignore[attr-defined]
    ctx.fell_back = False  # type: ignore[attr-defined]
    try:
        yield ctx
    finally:
        deactivate()


@contextmanager
def span_scope(name: str, *, attrs: dict[str, Any] | None = None) -> Iterator[Span]:
    """Open/close a span around a block, classifying any raised exception."""
    span = start_span(name)
    try:
        yield span
    except BaseException as exc:
        fields = _error_fields(exc)
        finish_span(
            span,
            error_category=fields["error_category"],
            error_message=fields["error_message"],
            cause=fields["cause"],
            attrs=attrs,
        )
        raise
    else:
        finish_span(span, attrs=attrs)


@contextmanager
def logical_call(name: str) -> Iterator[Span]:
    """One logical downstream call — retries and fallback nest underneath."""
    with span_scope(name) as span:
        yield span


def _attempt_span_name(name: str, index: int) -> str:
    return f"{name}/attempt-{index}"


def call_with_retry(
    fn: Callable[..., Any],
    name: str,
    *args: Any,
    attempts: int = 1,
    fallback: Callable[[Exception], Any] | None = None,
    **kwargs: Any,
) -> Any:
    """Invoke a sync callable with retries under one logical span.

    One logical call regardless of attempt count; internal retries never add
    extra entry-level calls in the metrics.
    """
    attempts = max(1, attempts)
    ctx = current_context()
    holder: list[Exception | None] = [None]

    def execute(call: Callable[[], Any]) -> Any:
        return call()

    def attempt_call() -> Any:
        return fn(*args, **kwargs)

    fallback_call: Callable[[], Any] | None = (
        (lambda: fallback(holder[0])) if fallback is not None else None  # type: ignore[arg-type]
    )

    with logical_call(name) as parent:
        for index in range(1, attempts + 1):
            try:
                with span_scope(_attempt_span_name(name, index)):
                    return execute(attempt_call)
            except Exception as exc:  # noqa: BLE001 - retried & re-classified
                holder[0] = exc
                if index < attempts:
                    # Account the extra physical attempt immediately, so the
                    # counter is correct even when the final try succeeds.
                    if ctx is not None:
                        ctx.retry_attempts += 1  # type: ignore[attr-defined]
                    continue
        if fallback_call is not None:
            if ctx is not None:
                ctx.fell_back = True  # type: ignore[attr-defined]
            try:
                with span_scope(f"{name}/fallback"):
                    return execute(fallback_call)
            except BaseException as fb_exc:
                parent.attrs["outcome"] = "fallback_failed"
                raise classify(fb_exc)
        parent.attrs["outcome"] = "failed"
        raise classify(holder[0])  # type: ignore[arg-type]


async def call_with_retry_async(
    fn: Callable[..., Any],
    name: str,
    *args: Any,
    attempts: int = 1,
    fallback: Callable[[Exception], Any] | None = None,
    **kwargs: Any,
) -> Any:
    """Invoke an async/sync callable with retries under one logical span.

    A sync or coroutine fallback is transparently awaited.
    """
    attempts = max(1, attempts)
    ctx = current_context()
    holder: list[Exception | None] = [None]

    async def execute(call: Callable[[], Any]) -> Any:
        result = call()
        if inspect.isawaitable(result):
            result = await result
        return result

    def attempt_call() -> Any:
        return fn(*args, **kwargs)

    fallback_call: Callable[[], Any] | None = (
        (lambda: fallback(holder[0])) if fallback is not None else None  # type: ignore[arg-type]
    )

    with logical_call(name) as parent:
        for index in range(1, attempts + 1):
            try:
                with span_scope(_attempt_span_name(name, index)):
                    return await execute(attempt_call)
            except Exception as exc:  # noqa: BLE001 - retried & re-classified
                holder[0] = exc
                if index < attempts:
                    # Account the extra physical attempt immediately, so the
                    # counter is correct even when the final try succeeds.
                    if ctx is not None:
                        ctx.retry_attempts += 1  # type: ignore[attr-defined]
                    continue
        if fallback_call is not None:
            if ctx is not None:
                ctx.fell_back = True  # type: ignore[attr-defined]
            try:
                with span_scope(f"{name}/fallback"):
                    return await execute(fallback_call)
            except BaseException as fb_exc:
                parent.attrs["outcome"] = "fallback_failed"
                raise classify(fb_exc)
        parent.attrs["outcome"] = "failed"
        raise classify(holder[0])  # type: ignore[arg-type]
