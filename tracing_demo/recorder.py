"""本地追踪记录存储：有界、线程安全、可本地查看。

最近 ``max_records`` 条 trace 记录，超出后按 FIFO 淘汰最早记录，
因此内存占用有明确上界。所有读写均在锁内完成，支持并发写入。
"""
from __future__ import annotations

import contextlib
import threading
from collections import OrderedDict

from tracing_demo.context import Span
from tracing_demo.errors import ErrorRecord


class TraceRecord:
    """一次入口请求的完整追踪记录。

    spans 可能在记录保存后仍由后台线程追加（后台任务沿用同一 trace），
    因此 :meth:`to_dict` 通过 ``span_lock`` 加锁快照；若记录与活跃
    :class:`~tracing_demo.context.TraceContext` 共享锁，则读取始终一致。
    """

    __slots__ = (
        "trace_id", "entry", "started_at", "duration_ms", "status",
        "spans", "errors", "span_lock",
    )

    def __init__(
        self,
        trace_id: str,
        entry: str,
        started_at: float,
        duration_ms: float,
        status: str,
        spans: list[Span] | None = None,
        errors: list[ErrorRecord] | None = None,
        span_lock: threading.Lock | None = None,
    ) -> None:
        self.trace_id = trace_id
        self.entry = entry
        self.started_at = started_at
        self.duration_ms = duration_ms
        self.status = status
        self.spans = spans or []
        self.errors = errors or []
        self.span_lock = span_lock

    def to_dict(self) -> dict:
        with self.span_lock or _null_lock():
            spans_snapshot = list(self.spans)
        return {
            "trace_id": self.trace_id,
            "entry": self.entry,
            "started_at": self.started_at,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "spans": [
                {
                    "span_id": s.span_id,
                    "parent_id": s.parent_id,
                    "name": s.name,
                    "started_at": s.started_at,
                    "duration_ms": s.duration_ms,
                    "status": s.status.value,
                    "error_category": s.error_category,
                    "attempt": s.attempt,
                }
                for s in spans_snapshot
            ],
            "errors": [e.to_dict() for e in self.errors],
        }


@contextlib.contextmanager
def _null_lock():
    yield


class TraceRecorder:
    """最近 N 条 trace 记录的有界存储（FIFO 淘汰）。"""

    def __init__(self, max_records: int = 1024) -> None:
        if max_records <= 0:
            raise ValueError("max_records must be positive")
        self.max_records = max_records
        self._records: OrderedDict[str, TraceRecord] = OrderedDict()
        self._lock = threading.Lock()

    def save(self, record: TraceRecord) -> None:
        with self._lock:
            # 同 trace 重复保存只做更新，不额外占用容量。
            exists = record.trace_id in self._records
            self._records[record.trace_id] = record
            if not exists:
                while len(self._records) > self.max_records:
                    self._records.popitem(last=False)

    def get(self, trace_id: str) -> TraceRecord | None:
        with self._lock:
            return self._records.get(trace_id)

    def all(self) -> list[TraceRecord]:
        """返回按保存时间从旧到新排列的记录快照副本。"""
        with self._lock:
            return list(self._records.values())

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)

    def reset(self) -> None:
        with self._lock:
            self._records.clear()


_recorder: TraceRecorder | None = None
_recorder_lock = threading.Lock()


def get_recorder() -> TraceRecorder:
    """进程内全局单例 recorder（测试可用 :func:`reset_recorder` 复位）。"""
    global _recorder
    with _recorder_lock:
        if _recorder is None:
            _recorder = TraceRecorder()
        return _recorder


def reset_recorder() -> None:
    global _recorder
    with _recorder_lock:
        if _recorder is not None:
            _recorder.reset()
        _recorder = None
