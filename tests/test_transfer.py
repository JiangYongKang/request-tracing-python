"""导出 / 导入（追踪数据可携带）行为测试。

日志中打印输入条件、trace_id 与归因结论，便于人工核对。
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time

import httpx
import pytest

from tracing_demo.app import app
from tracing_demo.context import Span, SpanStatus
from tracing_demo.errors import ErrorRecord
from tracing_demo.metrics import MetricsRegistry, get_metrics
from tracing_demo.recorder import TraceRecord, TraceRecorder, get_recorder
from tracing_demo.transfer import (
    CURRENT_VERSION,
    ExportFilter,
    export_traces,
    import_traces,
)

log = logging.getLogger("test.transfer")


def _make_record(
    trace_id: str,
    entry: str = "/orders",
    started_at: float = 1000.0,
    duration_ms: float = 120.0,
    status: str = "ok",
    error_category: str | None = None,
) -> TraceRecord:
    spans = [Span(span_id=f"{trace_id}-root", parent_id=None, name=f"entry:{entry}",
                  started_at=1.0, ended_at=1.0 + duration_ms / 1000.0)]
    errors: list[ErrorRecord] = []
    if error_category is not None:
        spans.append(Span(span_id=f"{trace_id}-child", parent_id=f"{trace_id}-root",
                          name="dep:inventory.reserve", started_at=1.01, ended_at=1.05,
                          status=SpanStatus.ERROR, error_category=error_category, attempt=3))
        errors.append(ErrorRecord(category=error_category, public_message="dep unavailable",
                                  cause_type="ConnectionError", cause_message="raw socket reset",
                                  span_name="dep:inventory.reserve", trace_id=trace_id))
    return TraceRecord(trace_id, entry, started_at, duration_ms, status,
                       spans=spans, errors=errors)


def _read_file_records(path: str) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as fh:
        header = json.loads(fh.readline())
        assert header["format"] == "trace-export"
        for line in fh:
            item = json.loads(line)
            if item["type"] == "record":
                records.append(item["record"])
    return records


def _assert_record_consistent(record: dict) -> None:
    """单条链路自洽：父子闭包、根 span 唯一、归因类别合法、错误可挂到 span。"""
    span_ids = [s["span_id"] for s in record["spans"]]
    if not span_ids:
        return  # 记录已保存但尚无 span（快照瞬间合法），不构成半条链路
    assert len(span_ids) == len(set(span_ids)), f"duplicate span ids in {record['trace_id']}"
    id_set = set(span_ids)
    roots = [s for s in record["spans"] if s["parent_id"] is None]
    assert len(roots) == 1, f"trace {record['trace_id']} must have exactly one root span"
    for s in record["spans"]:
        if s["parent_id"] is not None:
            assert s["parent_id"] in id_set, (
                f"span {s['span_id']} parent {s['parent_id']} missing -> 半条链路")
        if s["error_category"] is not None:
            assert s["error_category"] in {"validation", "business", "downstream", "internal"}
    for e in record["errors"]:
        assert e["trace_id"] == record["trace_id"]
        assert e["span_name"] in {s["name"] for s in record["spans"]}, (
            f"error attribution span {e['span_name']} not in trace {record['trace_id']}")


# ---------------------------------------------------------------------------
# 筛选导出
# ---------------------------------------------------------------------------
def test_export_filters_by_entry_time_status_and_category(tmp_path):
    rec = TraceRecorder()
    rec.save(_make_record("t-ok", entry="/orders", started_at=1000.0, status="ok"))
    rec.save(_make_record("t-fail-down", entry="/orders", started_at=1001.0,
                          status="error", error_category="downstream"))
    rec.save(_make_record("t-fail-biz", entry="/orders", started_at=1002.0,
                          status="error", error_category="business"))
    rec.save(_make_record("t-other-entry", entry="/status", started_at=1003.0,
                          status="error", error_category="downstream"))
    log.info("输入: 4 条 trace, 筛选 entry=/orders + only_failed + category=downstream")

    path = str(tmp_path / "out.jsonl")
    result = export_traces(rec, path, ExportFilter(
        entry="/orders", only_failed=True, error_category="downstream"))
    assert result.ok and result.record_count == 1
    records = _read_file_records(path)
    assert [r["trace_id"] for r in records] == ["t-fail-down"]
    log.info("导出命中 trace_id=%s, 归因=%s",
             records[0]["trace_id"], records[0]["errors"][0]["category"])

    # 时间范围筛选
    path2 = str(tmp_path / "out2.jsonl")
    result = export_traces(rec, path2, ExportFilter(started_after=1000.5, started_before=1002.5))
    assert result.ok
    assert {r["trace_id"] for r in _read_file_records(path2)} == {"t-fail-down", "t-fail-biz"}


# ---------------------------------------------------------------------------
# 并发导出快照一致
# ---------------------------------------------------------------------------
def test_export_snapshot_consistent_while_spans_appended(tmp_path):
    """记录保存后仍有后台线程追加 span 时，导出的每条链路必须自洽。"""
    import threading as th

    rec = TraceRecorder()
    stop = th.Event()

    lock = th.Lock()
    live_spans: list[Span] = []
    record = TraceRecord("t-live", "/orders", time.time(), 1.0, "ok",
                         spans=live_spans, span_lock=lock)
    rec.save(record)

    def writer():
        parent: Span | None = None
        i = 0
        while not stop.is_set():
            with lock:
                span = Span(span_id=f"s{i}", parent_id=parent.span_id if parent else None,
                            name=f"background:audit:{i}", started_at=1.0, ended_at=1.001)
                live_spans.append(span)
                parent = span
                i += 1

    t = th.Thread(target=writer)
    t.start()
    try:
        for n in range(20):
            path = str(tmp_path / f"snap{n}.jsonl")
            result = export_traces(rec, path)
            assert result.ok
            for r in _read_file_records(path):
                _assert_record_consistent(r)
    finally:
        stop.set()
        t.join()
    log.info("并发追加期间导出 20 次, trace_id=t-live 每次快照父子关系均闭合")


@pytest.mark.parametrize("rounds", [1])
def test_concurrent_requests_export_snapshot_consistent(tmp_path, rounds):
    """服务正常接并发请求 + 后台任务写链路时，导出文件内每条链路自洽。"""
    from tracing_demo import service

    async def drive():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            payloads = (
                [{"item_id": f"item-{i}", "qty": 1} for i in range(10)]
                + [{"item_id": "x", "qty": 1, "inventory_fail_times": 9} for _ in range(5)]
                + [{"item_id": "", "qty": 1} for _ in range(5)]
            )
            tasks = [asyncio.create_task(client.post("/orders", json=p)) for p in payloads]
            # 请求在途时并发导出多次
            exports = []
            for n in range(5):
                await asyncio.sleep(0)
                path = str(tmp_path / f"live{n}.jsonl")
                exports.append(await asyncio.to_thread(export_traces, get_recorder(), path))
            responses = await asyncio.gather(*tasks)
            return responses, exports

    responses, exports = asyncio.run(drive())
    for f in service.background_futures():
        f.result(timeout=5)

    log.info("输入: %d 并发请求(成功/降级/校验失败), 在途导出 %d 次", len(responses), len(exports))
    assert all(r.status_code in (200, 400) for r in responses)
    total = 0
    for n in range(5):
        path = str(tmp_path / f"live{n}.jsonl")
        for r in _read_file_records(path):
            _assert_record_consistent(r)
            total += 1
    log.info("在途导出共校验 %d 条链路, 父子关系与失败归因全部自洽", total)
    assert total > 0


# ---------------------------------------------------------------------------
# 幂等导入 & 不覆盖已有数据
# ---------------------------------------------------------------------------
def test_import_idempotent_and_never_overwrites(tmp_path):
    src = TraceRecorder()
    src.save(_make_record("t-1", started_at=1000.0, status="error", error_category="internal"))
    src.save(_make_record("t-2", started_at=1001.0))
    path = str(tmp_path / "f.jsonl")
    assert export_traces(src, path).ok

    dst = TraceRecorder()
    # 环境里已有同 trace_id 的不同内容（例如本地自己产生的）
    existing = _make_record("t-1", entry="/local", started_at=2000.0, duration_ms=9.0)
    dst.save(existing)
    metrics = MetricsRegistry()

    r1 = import_traces(dst, metrics, path)
    log.info("首次导入: %s", r1)
    assert r1.ok and r1.added == 1 and r1.skipped_existing == 1
    assert dst.get("t-1") is existing, "已有追踪数据不得被覆盖或改动"
    assert dst.get("t-1").entry == "/local"

    count_before = metrics.snapshot()["entries"]["/orders"]["count"]
    r2 = import_traces(dst, metrics, path)
    log.info("重复导入: %s", r2)
    assert r2.ok and r2.added == 0 and r2.skipped_existing == 2
    assert len(dst) == 2, "重复导入不能变成两份"
    assert metrics.snapshot()["entries"]["/orders"]["count"] == count_before, (
        "重复导入不得重复计数")


def test_import_respects_capacity_and_evicts_fifo(tmp_path):
    src = TraceRecorder()
    for i in range(10):
        src.save(_make_record(f"t-{i}", started_at=1000.0 + i))
    path = str(tmp_path / "f.jsonl")
    assert export_traces(src, path).ok

    dst = TraceRecorder(max_records=4)
    dst.save(_make_record("t-local", entry="/local"))
    result = import_traces(dst, MetricsRegistry(), path)
    log.info("容量 4 的环境导入 10 条: %s", result)
    assert result.ok and result.added == 10 and result.evicted == 7
    assert len(dst) == 4
    remaining = [r.trace_id for r in dst.all()]
    assert remaining == ["t-6", "t-7", "t-8", "t-9"], "超出上界按既有 FIFO 策略淘汰"


# ---------------------------------------------------------------------------
# 版本兼容 / 损坏文件
# ---------------------------------------------------------------------------
def _write_v1_file(path, records):
    """v1 内容：span 没有 attempt 字段。"""
    import hashlib
    hasher = hashlib.sha256()
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"format": "trace-export", "version": 1,
                             "created_at": time.time(), "filter": {}}) + "\n")
        for r in records:
            d = r.to_dict()
            for s in d["spans"]:
                s.pop("attempt", None)
            line = json.dumps({"type": "record", "record": d})
            hasher.update(line.encode())
            fh.write(line + "\n")
        fh.write(json.dumps({"type": "footer", "record_count": len(records),
                             "sha256": hasher.hexdigest()}) + "\n")


def test_import_compatible_old_version(tmp_path):
    path = str(tmp_path / "v1.jsonl")
    _write_v1_file(path, [_make_record("t-v1", status="error", error_category="business")])
    dst, metrics = TraceRecorder(), MetricsRegistry()
    result = import_traces(dst, metrics, path)
    log.info("导入 v1 文件: %s", result)
    assert result.ok and result.added == 1
    record = dst.get("t-v1").to_dict()
    assert all(s["attempt"] == 1 for s in record["spans"]), "v1 缺省字段迁移为默认值"
    assert record["errors"][0]["category"] == "business"
    log.info("v1 还原: trace_id=t-v1 归因=%s", record["errors"][0]["category"])


@pytest.mark.parametrize("case", ["unsupported_version", "truncated", "bad_checksum",
                                  "not_json", "not_export_file", "empty"])
def test_import_rejects_bad_files_without_dirty_state(tmp_path, case):
    good = str(tmp_path / "good.jsonl")
    src = TraceRecorder()
    src.save(_make_record("t-1"))
    assert export_traces(src, good).ok
    lines = open(good).read().splitlines()

    path = str(tmp_path / f"{case}.jsonl")
    if case == "unsupported_version":
        header = json.loads(lines[0])
        header["version"] = CURRENT_VERSION + 100
        open(path, "w").write("\n".join([json.dumps(header)] + lines[1:]) + "\n")
    elif case == "truncated":
        open(path, "w").write("\n".join(lines[:-1]) + "\n")  # 去掉 footer
    elif case == "bad_checksum":
        footer = json.loads(lines[-1])
        footer["sha256"] = "0" * 64
        open(path, "w").write("\n".join(lines[:-1] + [json.dumps(footer)]) + "\n")
    elif case == "not_json":
        open(path, "w").write("this is not json\n")
    elif case == "not_export_file":
        open(path, "w").write(json.dumps({"format": "other", "version": 2}) + "\n")
    elif case == "empty":
        open(path, "w").write("")

    dst, metrics = TraceRecorder(), MetricsRegistry()
    result = import_traces(dst, metrics, path)
    log.info("损坏/不兼容输入 case=%s -> %s", case, result.error)
    assert not result.ok and result.error, "必须给出明确失败结果"
    assert len(dst) == 0, "失败导入不得在环境中留下半份脏数据"
    assert metrics.snapshot()["entries"] == {}, "失败导入不得登记任何指标"


# ---------------------------------------------------------------------------
# 导入后聚合还原一致
# ---------------------------------------------------------------------------
def test_import_restores_entry_aggregation(tmp_path):
    src = TraceRecorder()
    specs = [
        ("a1", "/orders", 100.0, "ok", None),
        ("a2", "/orders", 200.0, "error", "downstream"),
        ("a3", "/orders", 300.0, "error", "business"),
        ("b1", "/status", 10.0, "ok", None),
    ]
    for i, (tid, entry, dur, status, cat) in enumerate(specs):
        src.save(_make_record(tid, entry=entry, started_at=1000.0 + i,
                              duration_ms=dur, status=status, error_category=cat))
    path = str(tmp_path / "agg.jsonl")
    assert export_traces(src, path).ok

    dst, metrics = TraceRecorder(), MetricsRegistry()
    result = import_traces(dst, metrics, path)
    assert result.ok and result.added == 4

    entries = metrics.snapshot()["entries"]
    orders = entries["/orders"]
    log.info("导入后 /orders 聚合: %s", orders)
    assert orders["count"] == 3 and orders["error_count"] == 2
    assert orders["min_ms"] == 100.0 and orders["max_ms"] == 300.0
    assert orders["avg_ms"] == pytest.approx(200.0)
    assert orders["p50_ms"] == pytest.approx(200.0)
    assert entries["/status"]["count"] == 1

    # 每条链路的父子关系 / 耗时 / 归因与导出端一致
    for tid, entry, dur, status, cat in [(s[0], s[1], s[2], s[3], s[4]) for s in specs]:
        got = dst.get(tid).to_dict()
        assert got["entry"] == entry and got["status"] == status
        assert got["duration_ms"] == pytest.approx(dur)
        _assert_record_consistent(got)
        if cat:
            assert got["errors"][0]["category"] == cat
            log.info("trace_id=%s 归因还原: %s", tid, cat)
