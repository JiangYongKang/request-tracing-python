"""追踪上下文：Trace/Span 的生成与跨同步、异步、线程传播。

基于 :mod:`contextvars`，每个入口请求拥有独立 trace_id；
span 保留父子关系。
"""
from __future__ import annotations

import contextlib
import threading
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterator

from tracing_demo.errors import ErrorRecord, classify, make_error_record


class SpanStatus(str, Enum):
    OK = "ok"
    ERROR = "error"


@dataclass
class Span:
    span_id: str
    parent_id: str | None
    name: str
    started_at: float = 0.0
    ended_at: float = 0.0
    status: SpanStatus = SpanStatus.OK
    error_category: str | None = None
    attempt: int = 1

    @property
    def duration_ms(self) -> float:
        return max(0.0, (self.ended_at - self.started_at) * 1000.0)


@dataclass
class TraceContext:
    trace_id: str
    entry: str = ""
    root_span_id: str | None = None
    spans: list[Span] = field(default_factory=list)
    errors: list[ErrorRecord] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


_current_ctx: ContextVar[TraceContext | None] = ContextVar(
    "trace_context", default=None
)
# 当前打开的 span 链：使用不可变 tuple。
# asyncio 任务在创建时会复制 contextvars，tuple 不共享可变状态，
# 并发交错执行时每个协程恢复到各自的父 span，不会串号。
_span_stack: ContextVar[tuple[Span, ...]] = ContextVar(
    "span_stack", default=()
)


def current_context() -> TraceContext | None:
    return _current_ctx.get()


def current_trace_id() -> str | None:
    ctx = _current_ctx.get()
    return ctx.trace_id if ctx is not None else None


def start_trace(entry: str) -> tuple[TraceContext, object]:
    """为入口请求创建全新的追踪上下文并绑定到当前执行上下文。

    返回 ``(context, reset_token)``，入口处理结束后必须调用
    :func:`detach_trace` 复位，防止上下文泄漏到后续请求。
    """
    ctx = TraceContext(trace_id=uuid.uuid4().hex, entry=entry)
    token = _current_ctx.set(ctx)
    _span_stack.set(())
    return ctx, token


def detach_trace(token: object) -> None:
    """恢复入口上下文绑定。"""
    _current_ctx.reset(token)
    _span_stack.set(())


@contextlib.contextmanager
def use_span(name: str, *, attempt: int = 1) -> Iterator[Span]:
    """在当前 trace 下开启一个子 span，自动挂接父子关系。

    块内抛出异常时 span 标记为 ERROR 并记录归因类别，异常继续向上抛出；
    正常退出标记为 OK。父 span 取当前执行上下文栈顶；若栈为空
    （如后台线程首次开 span）则挂到入口根 span 下。
    """
    ctx = _current_ctx.get()
    if ctx is None:
        raise RuntimeError("use_span called outside of a trace context")

    stack = _span_stack.get()
    parent = stack[-1] if stack else None
    parent_id = parent.span_id if parent is not None else ctx.root_span_id
    span = Span(
        span_id=uuid.uuid4().hex,
        parent_id=parent_id,
        name=name,
        started_at=time.perf_counter(),
        attempt=attempt,
    )
    # 第一个 span 即入口根 span。
    if ctx.root_span_id is None:
        ctx.root_span_id = span.span_id
    _span_stack.set(stack + (span,))
    try:
        with ctx.lock:
            ctx.spans.append(span)
        yield span
    except BaseException as exc:  # 含 CancelledError 等
        span.status = SpanStatus.ERROR
        span.error_category = classify(exc).value
        raise
    finally:
        span.ended_at = time.perf_counter()
        _span_stack.set(stack)


def bind_context(ctx: TraceContext):
    """把已有上下文绑定到当前执行上下文（用于后台线程/任务）。

    返回 reset token，由 :func:`detach_trace` 复位。span 栈重置为空，
    因此后台任务内开启的 span 会挂到入口根 span 下。
    """
    token = _current_ctx.set(ctx)
    _span_stack.set(())
    return token


def record_error(exc: BaseException, *, span_name: str) -> None:
    """把失败归因记录写入当前 trace（含原始原因，供本地回溯）。"""
    ctx = _current_ctx.get()
    if ctx is None:
        raise RuntimeError("record_error called outside of a trace context")
    record = make_error_record(exc, span_name=span_name, trace_id=ctx.trace_id)
    with ctx.lock:
        ctx.errors.append(record)
