"""上一轮核心能力回归：上下文传播、失败归因、指标去重、记录有界。"""
from __future__ import annotations

import asyncio
import logging
import threading

import pytest

from tracing_demo import context as ctxmod
from tracing_demo.context import start_trace, detach_trace, use_span
from tracing_demo.errors import ErrorCategory, TracedError, as_traced, classify, to_public_message
from tracing_demo.metrics import MetricsRegistry
from tracing_demo.recorder import TraceRecord, TraceRecorder
from tracing_demo import service

log = logging.getLogger("test.core")


def test_span_parent_child_across_sync_async_and_thread():
    ctx, token = start_trace("/orders")
    try:
        async def main():
            with use_span("entry:/orders"):
                with use_span("business:reserve"):
                    pass
                await asyncio.sleep(0)
                with use_span("business:charge"):
                    pass
            # 后台线程沿用同一 trace
            done = threading.Event()

            def bg():
                t = ctxmod.bind_context(ctx)
                try:
                    with use_span("background:audit"):
                        pass
                finally:
                    ctxmod.detach_trace(t)
                    done.set()

            th = threading.Thread(target=bg)
            th.start()
            done.wait(5)
            th.join()

        asyncio.run(main())
        names = {s.name: s for s in ctx.spans}
        root = names["entry:/orders"]
        assert ctx.root_span_id == root.span_id
        assert names["business:reserve"].parent_id == root.span_id
        assert names["business:charge"].parent_id == root.span_id
        assert names["background:audit"].parent_id == root.span_id
        log.info("trace_id=%s 父子关系: root=%s, spans=%s",
                 ctx.trace_id, root.span_id[:8], sorted(names))
    finally:
        detach_trace(token)


def test_concurrent_async_spans_do_not_cross():
    """并发协程的 span 不串号：各自挂到各自的父。"""
    ctx, token = start_trace("/x")
    try:
        async def worker(tag):
            with use_span(f"parent:{tag}"):
                await asyncio.sleep(0)
                with use_span(f"child:{tag}"):
                    await asyncio.sleep(0)

        async def main():
            await asyncio.gather(*[worker(i) for i in range(10)])

        asyncio.run(main())
        by_name = {s.name: s for s in ctx.spans}
        for i in range(10):
            assert by_name[f"child:{i}"].parent_id == by_name[f"parent:{i}"].span_id
        log.info("trace_id=%s 10 组并发父子 span 均未串号", ctx.trace_id)
    finally:
        detach_trace(token)


def test_error_attribution_stable_and_sanitized():
    raw = ConnectionError("raw socket reset 10.0.0.1:5432")
    assert classify(raw) is ErrorCategory.INTERNAL
    assert to_public_message(raw) == "internal error"
    traced = as_traced(raw)
    assert traced.category is ErrorCategory.INTERNAL and traced.cause is raw

    biz = TracedError(ErrorCategory.BUSINESS, "item restricted", cause=ValueError("raw"))
    assert classify(biz) is ErrorCategory.BUSINESS
    assert to_public_message(biz) == "item restricted"
    log.info("归因: 原始异常->%s, 业务异常->%s, 对外文案均脱敏",
             classify(raw).value, classify(biz).value)


def test_metrics_retry_and_fallback_not_double_counted():
    m = MetricsRegistry()
    # 同一逻辑调用重试 3 次（2 次失败）+ 降级后的本地决策不再产生下游调用
    for attempt in range(1, 4):
        m.record_dependency("trace-1", "inventory", "reserve#item#2",
                            is_error=attempt < 3, is_retry=attempt > 1)
    deps = m.snapshot()["dependencies"]["inventory"]
    log.info("重试 3 次后聚合: %s", deps)
    assert deps["logical_calls"] == 1, "同一逻辑调用只计一次"
    assert deps["attempts"] == 3 and deps["failures"] == 2

    # 不同 trace 的同一逻辑键是新的逻辑调用
    m.record_dependency("trace-2", "inventory", "reserve#item#2", is_error=False)
    assert m.snapshot()["dependencies"]["inventory"]["logical_calls"] == 2


def test_metrics_bounds_and_entry_stats():
    m = MetricsRegistry(max_endpoints=2, max_samples=5, max_tracked_traces=2)
    for i in range(3):
        m.record_entry(f"/e{i}", 10.0 * (i + 1), is_error=(i == 0))
    snap = m.snapshot()["entries"]
    assert list(snap) == ["/e1", "/e2"], "端点数超界按 LRU 淘汰"
    assert snap["/e1"]["count"] == 1 and snap["/e1"]["error_count"] == 0
    log.info("入口聚合(有界): %s", snap)


def test_recorder_capacity_fifo_and_update_in_place():
    rec = TraceRecorder(max_records=3)
    for i in range(5):
        rec.save(TraceRecord(f"t{i}", "/x", float(i), 1.0, "ok"))
    assert [r.trace_id for r in rec.all()] == ["t2", "t3", "t4"], "FIFO 淘汰最早记录"
    rec.save(TraceRecord("t3", "/x", 3.0, 2.0, "error"))  # 同 id 更新不占容量
    assert len(rec) == 3 and rec.get("t3").status == "error"
    log.info("recorder 容量 3, 现存: %s", [r.trace_id for r in rec.all()])


def test_service_paths_attribution():
    """业务链路：成功 / 降级 / 校验失败 / 业务失败 / 下游失败归因互不相同。"""
    async def run(payload):
        ctx, token = start_trace("/orders")
        try:
            try:
                result = await service.submit_order(payload)
                return "ok", result, ctx
            except TracedError as exc:
                return exc.category.value, exc, ctx
        finally:
            detach_trace(token)

    status, result, ctx = asyncio.run(run({"item_id": "a", "qty": 2}))
    assert status == "ok" and result["reservation"]["via"] == "inventory-service"

    status, result, ctx = asyncio.run(run({"item_id": "a", "qty": 2, "inventory_fail_times": 9}))
    assert status == "ok" and result["reservation"]["degraded"] is True
    log.info("降级链路: via=%s", result["reservation"]["via"])

    status, exc, _ = asyncio.run(run({"item_id": "", "qty": 2}))
    assert status == "validation"
    status, exc, _ = asyncio.run(run({"item_id": service.RESTRICTED_ITEM, "qty": 1}))
    assert status == "business"
    status, exc, _ = asyncio.run(run({"item_id": "a", "qty": 1, "payment_fail_times": 9}))
    assert status == "downstream"
    log.info("归因结论: validation/business/downstream 三类失败路径均稳定")
