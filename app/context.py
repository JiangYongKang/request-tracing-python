"""Trace context: ids, spans, parent-child relations, contextvar carrier.

Propagation model
-----------------
* The current :class:`TraceContext` lives in a :class:`contextvars.ContextVar`,
  so ``asyncio`` tasks (which copy the calling context at creation) and threads
  started through :func:`copy_context` all carry the same trace object without
  any global mutable "current request" state.
* The active-span stack is a *separate* ContextVar holding an immutable tuple.
  Each execution branch that opens a span pushes onto its own copy of the
  tuple, hence concurrent/interleaved branches share the trace but never
  cross-link each other's parent ids.
"""
from __future__ import annotations

import asyncio
import contextvars
import itertools
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar

T = TypeVar("T")

_seq_lock = threading.Lock()
_seq = itertools.count(1)

#: Trace bound to the current execution branch.
_current: contextvars.ContextVar[TraceContext | None] = contextvars.ContextVar(
    "trace_context", default=None
)
#: Active stack of open span ids on the current execution branch.
_active_stack: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar(
    "active_span_stack", default=()
)
#: Error that an inner framework handler converted into a response, so the
#: outer middleware attributes the trace consistently despite catching
#: nothing itself.
_error: contextvars.ContextVar[BaseException | None] = contextvars.ContextVar(
    "trace_error", default=None
)


def new_trace_id() -> str:
    """Return a globally unique, opaque trace id."""
    return uuid.uuid4().hex


def new_span_id() -> str:
    """Return a process-unique, monotonic-looking span id."""
    with _seq_lock:
        return f"{next(_seq):012x}"


@dataclass
class Span:
    span_id: str
    parent_id: str | None
    name: str
    start_perf: float = 0.0
    end_perf: float | None = None
    error_category: str | None = None
    error_message: str | None = None
    cause_type: str | None = None
    cause_message: str | None = None
    attrs: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float | None:
        if self.end_perf is None:
            return None
        return (self.end_perf - self.start_perf) * 1000.0

    def snapshot(self) -> dict[str, Any]:
        return {
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "name": self.name,
            "duration_ms": round(self.duration_ms, 3) if self.end_perf is not None else None,
            "error_category": self.error_category,
            "error_message": self.error_message,
            "cause_type": self.cause_type,
            "cause_message": self.cause_message,
            "attrs": dict(self.attrs),
        }


@dataclass
class TraceContext:
    trace_id: str
    root_span_id: str
    spans: list[Span] = field(default_factory=list)
    entry: str | None = None
    created_at: float = 0.0
    #: Set by middleware so post-response background work can keep appending
    #: spans; awaited before the trace record is snapshotted & persisted.
    pending: int = 0
    completion: asyncio.Event | None = None

    def find_span(self, span_id: str) -> Span | None:
        for span in self.spans:
            if span.span_id == span_id:
                return span
        return None

    def snapshot(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "entry": self.entry,
            "created_at": self.created_at,
            "root_span_id": self.root_span_id,
            "spans": [span.snapshot() for span in self.spans],
        }


def current_context() -> TraceContext | None:
    return _current.get()


def report_error(exc: BaseException) -> None:
    """Record that an inner handler turned ``exc`` into a response."""
    _error.set(exc)


def reported_error() -> BaseException | None:
    return _error.get()


def current_active_span_id() -> str | None:
    stack = _active_stack.get()
    return stack[-1] if stack else None


def start_trace(entry: str | None = None) -> tuple[TraceContext, Callable[[], None]]:
    """Create a trace, activate it on the current branch.

    Returns the context and a deactivation callable that restores the previous
    state (used in ``finally`` blocks).
    """
    trace_id = new_trace_id()
    root = Span(
        span_id=new_span_id(),
        parent_id=None,
        name=entry or "entry",
        start_perf=time.perf_counter(),
    )
    ctx = TraceContext(
        trace_id=trace_id,
        root_span_id=root.span_id,
        spans=[root],
        entry=entry,
        created_at=time.time(),
    )
    ctx_token = _current.set(ctx)
    stack_token = _active_stack.set((root.span_id,))

    def deactivate() -> None:
        _active_stack.reset(stack_token)
        _current.reset(ctx_token)

    return ctx, deactivate


def set_entry_name(entry: str) -> None:
    """Rename the entry (e.g. with the matched route template after routing)."""
    ctx = _current.get()
    if ctx is not None:
        ctx.entry = entry
        root = ctx.find_span(ctx.root_span_id)
        if root is not None and root.name == "entry":
            root.name = entry


def enable_completion(ctx: TraceContext) -> None:
    """Attach the completion gate used for post-response background work."""
    ctx.pending = 0
    ctx.completion = asyncio.Event()


def begin_background() -> None:
    """Mark one more unit of background work on the current trace."""
    ctx = _current.get()
    if ctx is not None and ctx.completion is not None:
        ctx.pending += 1


def end_background() -> None:
    """Finish one unit; release the gate when all units are done."""
    ctx = _current.get()
    if ctx is not None and ctx.completion is not None:
        ctx.pending = max(0, ctx.pending - 1)
        if ctx.pending == 0:
            ctx.completion.set()


def start_span(name: str) -> Span:
    """Open a child span under the active span on this branch."""
    ctx = _current.get()
    if ctx is None:
        raise RuntimeError("start_span() called without an active trace")
    stack = _active_stack.get()
    parent_id = stack[-1] if stack else ctx.root_span_id
    span = Span(
        span_id=new_span_id(),
        parent_id=parent_id,
        name=name,
        start_perf=time.perf_counter(),
    )
    ctx.spans.append(span)
    # ContextVar holds an immutable tuple; the set/reset only affects this
    # branch's context copy, never sibling branches sharing the trace.
    token = _active_stack.set(stack + (span.span_id,))
    span.attrs["_stack_token"] = token
    return span


def finish_span(
    span: Span,
    *,
    error_category: str | None = None,
    error_message: str | None = None,
    cause: BaseException | None = None,
    attrs: dict[str, Any] | None = None,
) -> None:
    """Close a span, recording failure attribution and originating cause."""
    if span.end_perf is None:
        span.end_perf = time.perf_counter()
    if error_category is not None:
        span.error_category = error_category
    if error_message is not None:
        span.error_message = error_message
    if cause is not None:
        span.cause_type = type(cause).__name__
        span.cause_message = str(cause) or repr(cause)
    if attrs:
        span.attrs.update(attrs)
    token = span.attrs.pop("_stack_token", None)
    if token is not None:
        try:
            _active_stack.reset(token)
        except ValueError:
            # The span is finished from another branch/context than the one
            # that opened it; leave that branch's stack untouched.
            pass


def _run_in_fresh_branch(
    ctx: TraceContext,
    parent_span_id: str | None,
    fn: Callable[..., T],
    args: tuple,
    kwargs: dict,
) -> T:
    ctx_token = _current.set(ctx)
    stack_token = _active_stack.set((parent_span_id or ctx.root_span_id,))
    try:
        return fn(*args, **kwargs)
    finally:
        _active_stack.reset(stack_token)
        _current.reset(ctx_token)


def run_with_context(
    ctx: TraceContext,
    fn: Callable[..., T],
    *args: Any,
    parent_span_id: str | None = None,
    **kwargs: Any,
) -> T:
    """Run ``fn`` with ``ctx`` active, inside an isolated context copy.

    Intended for worker threads / background tasks: no trace from the caller's
    other work leaks in, and mutations of the span stack cannot leak out.
    """
    branch = contextvars.copy_context()
    return branch.run(
        _run_in_fresh_branch, ctx, parent_span_id, fn, args, kwargs
    )


async def run_with_context_async(
    ctx: TraceContext,
    fn: Callable[..., Any],
    *args: Any,
    parent_span_id: str | None = None,
    **kwargs: Any,
) -> Any:
    """``async`` counterpart; asyncio already propagates the context, but this
    helper makes the continuation explicit and attaches spans to ``parent``."""
    ctx_token = _current.set(ctx)
    stack_token = _active_stack.set((parent_span_id or ctx.root_span_id,))
    try:
        return await fn(*args, **kwargs)
    finally:
        _active_stack.reset(stack_token)
        _current.reset(ctx_token)
