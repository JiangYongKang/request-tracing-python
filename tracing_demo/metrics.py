"""本地耗时分布与调用次数聚合。

特性：
- 并发写入安全（所有更新在同一把锁内）；
- 内存有明确上界：每端点样本数 ``max_samples``、端点数 ``max_endpoints``、
  用于重试去重的 trace 集合 ``max_tracked_traces``；
- 重试或降级的同一逻辑调用不会被重复计数：区分
  ``logical_calls``（逻辑调用）与 ``attempts``（物理尝试次数，含重试）。

完全本地实现，不依赖任何外部采集服务。
"""
from __future__ import annotations

import threading
from collections import OrderedDict, deque
from dataclasses import dataclass


class LatencyStats:
    """单入口维度的耗时分布。样本保留最近 ``max_samples`` 条（有界）。"""

    __slots__ = ("count", "error_count", "total_ms", "min_ms", "max_ms", "_samples")

    def __init__(self, max_samples: int) -> None:
        self.count = 0
        self.error_count = 0
        self.total_ms = 0.0
        self.min_ms: float | None = None
        self.max_ms: float | None = None
        self._samples: deque[float] = deque(maxlen=max_samples)

    def add(self, duration_ms: float, *, is_error: bool) -> None:
        self.count += 1
        if is_error:
            self.error_count += 1
        self.total_ms += duration_ms
        self.min_ms = duration_ms if self.min_ms is None else min(self.min_ms, duration_ms)
        self.max_ms = duration_ms if self.max_ms is None else max(self.max_ms, duration_ms)
        self._samples.append(duration_ms)

    @property
    def avg_ms(self) -> float | None:
        return self.total_ms / self.count if self.count else None

    def percentile(self, p: float) -> float | None:
        """样本分位数（p 取 0~1，线性插值），无样本返回 None。"""
        if not self._samples:
            return None
        if not 0.0 <= p <= 1.0:
            raise ValueError("p must be within [0, 1]")
        values = sorted(self._samples)
        if len(values) == 1:
            return values[0]
        pos = p * (len(values) - 1)
        lo = int(pos)
        hi = min(lo + 1, len(values) - 1)
        frac = pos - lo
        return values[lo] + (values[hi] - values[lo]) * frac

    def to_dict(self) -> dict:
        return {
            "count": self.count,
            "error_count": self.error_count,
            "avg_ms": self.avg_ms,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "p50_ms": self.percentile(0.50),
            "p95_ms": self.percentile(0.95),
            "p99_ms": self.percentile(0.99),
        }


@dataclass
class DependencyStats:
    """单一下游依赖的调用计数。"""

    logical_calls: int = 0
    attempts: int = 0
    failures: int = 0

    def to_dict(self) -> dict:
        return {
            "logical_calls": self.logical_calls,
            "attempts": self.attempts,
            "failures": self.failures,
        }


class MetricsRegistry:
    def __init__(
        self,
        max_endpoints: int = 128,
        max_samples: int = 200,
        max_tracked_traces: int = 1024,
    ) -> None:
        if max_endpoints <= 0 or max_samples <= 0 or max_tracked_traces <= 0:
            raise ValueError("bounds must be positive")
        self.max_endpoints = max_endpoints
        self.max_samples = max_samples
        self.max_tracked_traces = max_tracked_traces
        self._entries: OrderedDict[str, LatencyStats] = OrderedDict()
        self._deps: OrderedDict[str, DependencyStats] = OrderedDict()
        # trace_id -> 该 trace 已计数的 (dependency, logical_key) 集合。
        self._trace_keys: OrderedDict[str, set[tuple[str, str]]] = OrderedDict()
        self._lock = threading.Lock()

    def record_entry(self, endpoint: str, duration_ms: float, *, is_error: bool) -> None:
        """记录一次入口请求的耗时与成败。"""
        with self._lock:
            stats = self._entries.get(endpoint)
            if stats is None:
                stats = self._make_room(self._entries)
                self._entries[endpoint] = stats
            else:
                self._entries.move_to_end(endpoint)
            stats.add(duration_ms, is_error=is_error)

    def record_dependency(
        self,
        trace_id: str,
        dependency: str,
        logical_key: str,
        *,
        is_error: bool,
        is_retry: bool = False,
    ) -> None:
        """记录一次下游物理尝试。

        - 每次调用都累加 ``attempts``（含重试），失败累加 ``failures``；
        - 同一 ``(trace_id, dependency, logical_key)`` 的 ``logical_calls``
          只累加一次，因此重试或降级路径上的重复尝试不会重复计数；
          ``is_retry`` 为显式声明，去重集为最终防线。
        """
        with self._lock:
            stats = self._deps.get(dependency)
            if stats is None:
                stats = self._make_room(self._deps, kind=DependencyStats)
                self._deps[dependency] = stats
            stats.attempts += 1
            if is_error:
                stats.failures += 1

            keys = self._trace_keys.get(trace_id)
            if keys is None:
                self._trace_keys[trace_id] = set()
                self._trace_keys.move_to_end(trace_id)
                while len(self._trace_keys) > self.max_tracked_traces:
                    self._trace_keys.popitem(last=False)
                keys = self._trace_keys[trace_id]
            key = (dependency, logical_key)
            if key not in keys:
                keys.add(key)
                stats.logical_calls += 1
            # is_retry 仅为调用方语义提示：若声明非重试但 key 已存在，
            # 仍不去重防线外重复计数。
            _ = is_retry

    def _make_room(self, table: OrderedDict, *, kind: type = LatencyStats):
        """为新 key 腾出容量：超出上界时淘汰 LRU 条目。"""
        if len(table) >= self.max_endpoints:
            table.popitem(last=False)
        if kind is DependencyStats:
            return DependencyStats()
        return LatencyStats(self.max_samples)

    def snapshot(self) -> dict:
        """返回可 JSON 序列化的聚合快照。"""
        with self._lock:
            return {
                "entries": {k: v.to_dict() for k, v in self._entries.items()},
                "dependencies": {k: v.to_dict() for k, v in self._deps.items()},
            }

    def reset(self) -> None:
        with self._lock:
            self._entries.clear()
            self._deps.clear()
            self._trace_keys.clear()


_metrics: MetricsRegistry | None = None
_metrics_lock = threading.Lock()


def get_metrics() -> MetricsRegistry:
    """进程内全局单例 registry（测试可用 :func:`reset_metrics` 复位）。"""
    global _metrics
    with _metrics_lock:
        if _metrics is None:
            _metrics = MetricsRegistry()
        return _metrics


def reset_metrics() -> None:
    global _metrics
    with _metrics_lock:
        if _metrics is not None:
            _metrics.reset()
        _metrics = None
