"""追踪数据导出/导入的自动化测试。

覆盖：并发导出快照一致、重复导入幂等、版本兼容与损坏文件处理、
导入后聚合结果还原一致。日志中打印输入、追踪标识与归因结论。
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import tempfile
import threading

import httpx
import pytest

from tracing_demo.app import app
from tracing_demo.exporter import EXPORT_FORMAT, EXPORT_VERSION, ExportFilter, export_traces
from tracing_demo.importer import import_traces
from tracing_demo.metrics import MetricsRegistry, get_metrics
from tracing_demo.recorder import TraceRecorder, TraceRecord, get_recorder
from tracing_demo.service import background_futures

log = logging.getLogger("tracing-test")

# ---------------------------------------------------------------------------
# 流量脚本：覆盖成功 / 校验失败 / 业务失败 / 下游失败(重试) / 降级 / 后台失败
# ---------------------------------------------------------------------------
CASES = [
    *[(f"order-ok-{i}", "POST", "/orders", {"item_id": f"item-ok-{i}", "qty": 2}, 200)
      for i in range(6)],
    ("order-validation-1", "POST", "/orders", {"item_id": "", "qty": 2}, 400),
    ("order-validation-2", "POST", "/orders", {"item_id": "item-x", "qty": -1}, 400),
    ("order-business-1", "POST", "/orders", {"item_id": "item-restricted", "qty": 1}, 409),
    ("order-business-2", "POST", "/orders", {"item_id": "item-restricted", "qty": 3}, 409),
    ("payment-downstream-1", "POST", "/payments",
     {"order_id": "o-fail-1", "amount": 5, "payment_fail_times": 9}, 502),
    ("payment-downstream-2", "POST", "/payments",
     {"order_id": "o-fail-2", "amount": 5, "payment_fail_times": 9}, 502),
    ("order-degraded-1", "POST", "/orders",
     {"item_id": "item-deg-1", "qty": 1, "inventory_fail_times": 9}, 200),
    ("order-degraded-2", "POST", "/orders",
     {"item_id": "item-deg-2", "qty": 1, "inventory_fail_times": 9}, 200),
    ("order-audit-fail", "POST", "/orders",
     {"item_id": "item-audit", "qty": 1, "audit_fail": True}, 200),
    *[(f"payment-ok-{i}", "POST", "/payments", {"order_id": f"o-ok-{i}", "amount": 9.9}, 200)
      for i in range(3)],
    *[(f"status-{i}", "GET", "/status", None, 200) for i in range(2)],
]


async def _drive_traffic() -> list[dict]:
    """并发打一遍混合流量，返回每个用例的输入与追踪标识/归因结论。"""
    transport = httpx.ASGITransport(app=app)
    outcomes: list[dict] = []

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async def one(name, method, path, payload, expect):
            resp = await client.request(method, path, json=payload)
            body = resp.json()
            outcome = {
                "case": name,
                "input": payload,
                "status_code": resp.status_code,
                "trace_id": body.get("trace_id"),
                "category": (body.get("error") or {}).get("category"),
            }
            assert resp.status_code == expect, outcome
            outcomes.append(outcome)

        await asyncio.gather(*[one(*c) for c in CASES])

    for o in outcomes:
        log.info("traffic case=%s input=%s -> http=%s trace_id=%s category=%s",
                 o["case"], o["input"], o["status_code"], o["trace_id"], o["category"])
    return outcomes


def _wait_background() -> None:
    for fut in background_futures():
        try:
            fut.result(timeout=5)
        except Exception as exc:  # 后台失败也应已留痕而非炸掉测试
            log.info("background future finished with: %r", exc)


def _read_export(path: str) -> tuple[dict, list[dict]]:
    with open(path, "r", encoding="utf-8") as fh:
        header = json.loads(fh.readline())
        records = [json.loads(line) for line in fh if line.strip()]
    return header, records


def _assert_record_consistent(rec: dict) -> None:
    """单条链路结构一致性：父子关系闭合、归因挂得上、失败必有归因。"""
    span_ids = [s["span_id"] for s in rec["spans"]]
    assert len(span_ids) == len(set(span_ids)), f"duplicate span ids in {rec['trace_id']}"
    id_set = set(span_ids)
    for span in rec["spans"]:
        assert span["parent_id"] is None or span["parent_id"] in id_set, (
            f"dangling parent {span['parent_id']} in trace {rec['trace_id']}")
    span_names = {s["name"] for s in rec["spans"]}
    for err in rec["errors"]:
        assert err["trace_id"] == rec["trace_id"]
        assert err["category"] in {"validation", "business", "downstream", "internal"}
        assert err["span_name"] in span_names, (
            f"error attribution spans mismatch in trace {rec['trace_id']}")
    if rec["status"] == "error":
        assert rec["errors"], f"failed trace {rec['trace_id']} has no attribution"
    if rec["errors"]:
        log.info("consistent trace_id=%s entry=%s status=%s categories=%s causes=%s",
                 rec["trace_id"], rec["entry"], rec["status"],
                 [e["category"] for e in rec["errors"]],
                 [e["cause_type"] for e in rec["errors"]])


def _deep_close(a, b, path: str = "root") -> None:
    """结构递归比较，浮点叶子允许 1e-9 相对误差。"""
    if isinstance(a, (int, float)) and not isinstance(a, bool) \
            and isinstance(b, (int, float)) and not isinstance(b, bool):
        assert math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-12), (
            f"{path}: {a} != {b}")
    elif isinstance(a, dict) and isinstance(b, dict):
        assert set(a) == set(b), f"{path}: keys {set(a)} != {set(b)}"
        for key in a:
            _deep_close(a[key], b[key], f"{path}.{key}")
    elif isinstance(a, list) and isinstance(b, list):
        assert len(a) == len(b), f"{path}: len {len(a)} != {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            _deep_close(x, y, f"{path}[{i}]")
    else:
        assert a == b, f"{path}: {a!r} != {b!r}"


def _tmp() -> str:
    fd, path = tempfile.mkstemp(suffix=".ndjson")
    os.close(fd)
    os.unlink(path)
    return path


# ---------------------------------------------------------------------------
# 1. 并发导出时快照一致
# ---------------------------------------------------------------------------
def test_export_snapshot_consistent_under_concurrency():
    asyncio.run(_drive_traffic_concurrently_with_exports())


async def _drive_traffic_concurrently_with_exports():
    exported: list[str] = []
    stop = threading.Event()

    def export_loop():
        while not stop.is_set():
            path = _tmp()
            if export_traces(get_recorder(), path).ok:
                exported.append(path)

    thread = threading.Thread(target=export_loop, name="concurrent-exporter")
    thread.start()
    try:
        await _drive_traffic()
    finally:
        stop.set()
        thread.join()
    _wait_background()

    # 导出期间的每一个文件：每条链路结构都必须闭合一致。
    total = 0
    for path in exported:
        header, records = _read_export(path)
        assert header["format"] == EXPORT_FORMAT
        assert header["version"] == EXPORT_VERSION
        for rec in records:
            _assert_record_consistent(rec)
        total += len(records)
        os.unlink(path)
    log.info("concurrent exports: files=%d records=%d, all consistent", len(exported), total)
    assert exported, "expected at least one concurrent export"

    # 后台任务收尾后再导一次：订单链路必须带上后台审计 span 且父子闭合。
    final_path = _tmp()
    result = export_traces(get_recorder(), final_path, ExportFilter(entry="POST /orders"))
    assert result.ok
    _, records = _read_export(final_path)
    os.unlink(final_path)
    ok_orders = [r for r in records if r["status"] == "ok"]
    assert ok_orders, "expected successful order traces"
    for rec in ok_orders:
        _assert_record_consistent(rec)
        names = [s["name"] for s in rec["spans"]]
        assert "background:audit" in names, f"trace {rec['trace_id']} missing audit span"
        by_id = {s["span_id"]: s for s in rec["spans"]}
        audit = next(s for s in rec["spans"] if s["name"] == "background:audit")
        child = next(s for s in rec["spans"] if s["name"] == "background:audit:write")
        assert child["parent_id"] == audit["span_id"]
        assert by_id[audit["parent_id"]]["name"].startswith("entry:")
    log.info("final export: %d ok order traces include background audit spans", len(ok_orders))


# ---------------------------------------------------------------------------
# 2. 重复导入幂等，且不覆盖已有数据
# ---------------------------------------------------------------------------
def test_import_idempotent_and_non_overwriting():
    asyncio.run(_drive_traffic())
    _wait_background()
    path = _tmp()
    result = export_traces(get_recorder(), path)
    assert result.ok and result.record_count > 0
    log.info("exported %d records for idempotency test", result.record_count)

    recorder, metrics = TraceRecorder(), MetricsRegistry()
    first = import_traces(recorder, metrics, path)
    assert first.ok and first.imported == result.record_count and first.skipped == 0
    records_after_first = {r.trace_id: r.to_dict() for r in recorder.all()}
    metrics_after_first = metrics.snapshot()

    second = import_traces(recorder, metrics, path)
    assert second.ok and second.imported == 0 and second.skipped == result.record_count
    assert {r.trace_id: r.to_dict() for r in recorder.all()} == records_after_first
    assert metrics.snapshot() == metrics_after_first
    log.info("re-import: imported=%d skipped=%d, recorder/metrics unchanged",
             second.imported, second.skipped)

    # 已有同 trace_id 的本地数据不得被覆盖或改动。
    _, records = _read_export(path)
    victim = records[0]
    recorder2, metrics2 = TraceRecorder(), MetricsRegistry()
    recorder2.save(TraceRecord(victim["trace_id"], victim["entry"], 0.0, 999.0, "ok"))
    third = import_traces(recorder2, metrics2, path)
    assert third.ok and third.skipped == 1 and third.imported == result.record_count - 1
    kept = recorder2.get(victim["trace_id"])
    assert kept is not None and kept.duration_ms == 999.0
    log.info("pre-existing trace_id=%s kept intact (duration_ms=999.0)", victim["trace_id"])
    os.unlink(path)


# ---------------------------------------------------------------------------
# 3. 版本兼容与损坏文件处理
# ---------------------------------------------------------------------------
def _write(path: str, *lines: str) -> str:
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return path


_HEADER = json.dumps({"format": EXPORT_FORMAT, "version": EXPORT_VERSION})


@pytest.mark.parametrize("kind", [
    "unsupported_version", "wrong_format", "truncated", "garbage_line",
    "empty", "missing_file", "bad_field_type",
])
def test_import_rejects_incompatible_or_corrupt_files(kind):
    recorder, metrics = TraceRecorder(), MetricsRegistry()
    path = _tmp()
    good_record = json.dumps({
        "trace_id": "t-good", "entry": "GET /status", "started_at": 1.0,
        "duration_ms": 2.0, "status": "ok", "spans": [], "errors": [],
    })
    if kind == "unsupported_version":
        _write(path, json.dumps({"format": EXPORT_FORMAT, "version": 99}), good_record)
    elif kind == "wrong_format":
        _write(path, json.dumps({"format": "other/format", "version": 1}), good_record)
    elif kind == "truncated":
        _write(path, _HEADER, good_record[: len(good_record) // 2])
    elif kind == "garbage_line":
        _write(path, _HEADER, good_record, "this is not json")
    elif kind == "empty":
        _write(path)
    elif kind == "missing_file":
        os.unlink(path) if os.path.exists(path) else None
        path = path + ".does-not-exist"
    elif kind == "bad_field_type":
        _write(path, _HEADER, json.dumps({"trace_id": 123, "entry": "GET /status"}))

    result = import_traces(recorder, metrics, path)
    assert not result.ok, f"{kind} should fail"
    assert result.error, f"{kind} should carry a clear error"
    # 环境不能留下半份脏数据。
    assert len(recorder) == 0
    assert metrics.snapshot() == {"entries": {}, "dependencies": {}}
    log.info("rejected kind=%s error=%s (env clean)", kind, result.error)
    if os.path.exists(path):
        os.unlink(path)


def test_import_restores_compatible_legacy_content():
    """版本兼容范围内的旧内容（缺可选字段）应带默认值正常还原。"""
    recorder, metrics = TraceRecorder(), MetricsRegistry()
    path = _tmp()
    legacy_record = json.dumps({
        "trace_id": "t-legacy", "entry": "GET /status", "started_at": 1.0,
        "duration_ms": 3.0, "status": "ok",
        # span 缺 attempt/status/duration_ms；error 缺 cause 字段。
        "spans": [{"span_id": "s1", "parent_id": None, "name": "entry:GET /status"}],
        "errors": [{"category": "internal", "span_name": "entry:GET /status"}],
    })
    _write(path, _HEADER, legacy_record)

    result = import_traces(recorder, metrics, path)
    assert result.ok and result.imported == 1, result
    restored = recorder.get("t-legacy").to_dict()
    span = restored["spans"][0]
    assert span["attempt"] == 1 and span["status"] == "ok" and span["duration_ms"] == 0.0
    error = restored["errors"][0]
    assert error["category"] == "internal" and error["cause_type"] is None
    log.info("legacy content restored: trace_id=%s span_defaults=%s error=%s",
             restored["trace_id"],
             {"attempt": span["attempt"], "status": span["status"]}, error)
    os.unlink(path)


# ---------------------------------------------------------------------------
# 4. 导入后聚合结果与记录内容还原一致
# ---------------------------------------------------------------------------
def test_import_restores_records_and_aggregates():
    asyncio.run(_drive_traffic())
    _wait_background()

    path = _tmp()
    result = export_traces(get_recorder(), path)
    assert result.ok
    _, exported_records = _read_export(path)
    source_metrics = get_metrics().snapshot()
    log.info("export for restore test: %d records, entries=%s",
             result.record_count,
             {k: v["count"] for k, v in source_metrics["entries"].items()})

    recorder, metrics = TraceRecorder(), MetricsRegistry()
    imported = import_traces(recorder, metrics, path)
    assert imported.ok and imported.imported == len(exported_records)

    # 每条链路：父子关系、耗时、失败归因（含原始原因）逐字段还原。
    for rec in exported_records:
        restored = recorder.get(rec["trace_id"])
        assert restored is not None, f"missing trace {rec['trace_id']}"
        _deep_close(rec, restored.to_dict(), path=f"trace {rec['trace_id']}")
        _assert_record_consistent(rec)
    log.info("all %d traces restored field-by-field (incl. causes)", len(exported_records))

    # 按入口的耗时分布与调用次数与导出端一致。
    restored_metrics = metrics.snapshot()
    assert set(restored_metrics["entries"]) == set(source_metrics["entries"])
    for entry, src in source_metrics["entries"].items():
        dst = restored_metrics["entries"][entry]
        _deep_close(src, dst, path=f"entry {entry}")
        log.info("entry=%s count=%d errors=%d avg_ms=%.3f p95_ms=%.3f (matches export side)",
                 entry, dst["count"], dst["error_count"], dst["avg_ms"], dst["p95_ms"])

    # 下游调用计数（重试/降级不重复计数）也一致。
    assert restored_metrics["dependencies"] == source_metrics["dependencies"]
    for dep, stats in restored_metrics["dependencies"].items():
        log.info("dependency=%s logical_calls=%d attempts=%d failures=%d (matches)",
                 dep, stats["logical_calls"], stats["attempts"], stats["failures"])
    os.unlink(path)


def test_export_filters_select_expected_subsets():
    asyncio.run(_drive_traffic())
    _wait_background()

    path = _tmp()
    only_failures = export_traces(
        get_recorder(), path, ExportFilter(failed_only=True))
    _, records = _read_export(path)
    assert only_failures.record_count == len(records) > 0
    assert all(r["status"] == "error" for r in records)

    downstream_only = export_traces(
        get_recorder(), path, ExportFilter(error_category="downstream"))
    _, records = _read_export(path)
    assert downstream_only.record_count == len(records) > 0
    for rec in records:
        assert "downstream" in {e["category"] for e in rec["errors"]}
        # 原始原因一并带走。
        assert any(e["cause_type"] for e in rec["errors"])
        log.info("filtered trace_id=%s category=downstream cause=%s",
                 rec["trace_id"], [e["cause_type"] for e in rec["errors"]])

    orders_only = export_traces(
        get_recorder(), path, ExportFilter(entry="POST /orders"))
    _, records = _read_export(path)
    assert orders_only.record_count == len(records) > 0
    assert all(r["entry"] == "POST /orders" for r in records)

    bounded = export_traces(
        get_recorder(), path, ExportFilter(since=0.0, until=1.0))  # 不可能的时间窗
    assert bounded.record_count == 0
    os.unlink(path)
