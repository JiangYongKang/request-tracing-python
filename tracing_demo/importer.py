"""追踪数据导入：从本地导出文件还原到当前环境（骨架）。

行为约定：
- 识别文件头版本；兼容范围内的旧内容尽量还原，不兼容/损坏/不完整
  一律返回明确失败，且不在环境中留下半份数据（先整体校验再应用）；
- 幂等：同一文件重复导入不会产生第二份记录，也不覆盖/改动已有数据；
- 容量：导入记录仍走 recorder 的容量上界与 FIFO 淘汰；
- 流式读取：逐行解析，文件大也不会一次性吃满内存。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass

from tracing_demo.context import Span, SpanStatus
from tracing_demo.errors import ErrorRecord
from tracing_demo.exporter import EXPORT_FORMAT, EXPORT_VERSION
from tracing_demo.recorder import TraceRecord

#: 本实现能读取的内容版本集合。
SUPPORTED_VERSIONS = frozenset({EXPORT_VERSION})


@dataclass
class ImportResult:
    ok: bool
    imported: int = 0
    skipped: int = 0
    error: str | None = None


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise ValueError(msg)


def _num(value, field: str) -> float:
    _require(isinstance(value, (int, float)) and not isinstance(value, bool),
             f"field '{field}' must be a number")
    return float(value)


def _parse_span(obj, line_no: int) -> Span:
    """解析一条 span。兼容旧内容：可选字段缺失时取默认值。"""
    _require(isinstance(obj, dict), f"line {line_no}: span must be an object")
    _require(isinstance(obj.get("span_id"), str) and obj["span_id"],
             f"line {line_no}: span.span_id must be a non-empty string")
    _require(isinstance(obj.get("name"), str) and obj["name"],
             f"line {line_no}: span.name must be a non-empty string")
    parent_id = obj.get("parent_id")
    _require(parent_id is None or isinstance(parent_id, str),
             f"line {line_no}: span.parent_id must be a string or null")
    started_at = _num(obj.get("started_at", 0.0), "span.started_at")
    duration_ms = _num(obj.get("duration_ms", 0.0), "span.duration_ms")
    status_raw = obj.get("status", SpanStatus.OK.value)
    try:
        status = SpanStatus(status_raw)
    except ValueError:
        raise ValueError(f"line {line_no}: unknown span status {status_raw!r}") from None
    error_category = obj.get("error_category")
    _require(error_category is None or isinstance(error_category, str),
             f"line {line_no}: span.error_category must be a string or null")
    attempt = obj.get("attempt", 1)
    _require(isinstance(attempt, int) and not isinstance(attempt, bool) and attempt >= 1,
             f"line {line_no}: span.attempt must be a positive integer")
    return Span(
        span_id=obj["span_id"],
        parent_id=parent_id,
        name=obj["name"],
        started_at=started_at,
        ended_at=started_at + duration_ms / 1000.0,
        status=status,
        error_category=error_category,
        attempt=attempt,
    )


def _parse_error(obj, line_no: int) -> ErrorRecord:
    """解析一条失败归因记录（含原始原因）。可选字段缺失取默认值。"""
    _require(isinstance(obj, dict), f"line {line_no}: error must be an object")
    _require(isinstance(obj.get("category"), str) and obj["category"],
             f"line {line_no}: error.category must be a non-empty string")
    for key in ("public_message", "cause_type", "cause_message"):
        _require(obj.get(key) is None or isinstance(obj.get(key), str),
                 f"line {line_no}: error.{key} must be a string or null")
    return ErrorRecord(
        category=obj["category"],
        public_message=obj.get("public_message") or "",
        cause_type=obj.get("cause_type"),
        cause_message=obj.get("cause_message"),
        span_name=obj.get("span_name") or "",
        trace_id=obj.get("trace_id") or "",
    )


def _parse_record(obj, line_no: int) -> TraceRecord:
    """把一行 JSON 还原为 :class:`TraceRecord`，结构不合法抛 ``ValueError``。"""
    _require(isinstance(obj, dict), f"line {line_no}: record must be an object")
    _require(isinstance(obj.get("trace_id"), str) and obj["trace_id"],
             f"line {line_no}: record.trace_id must be a non-empty string")
    _require(isinstance(obj.get("entry"), str) and obj["entry"],
             f"line {line_no}: record.entry must be a non-empty string")
    status = obj.get("status", "ok")
    _require(isinstance(status, str), f"line {line_no}: record.status must be a string")
    spans_raw = obj.get("spans", [])
    errors_raw = obj.get("errors", [])
    _require(isinstance(spans_raw, list), f"line {line_no}: record.spans must be a list")
    _require(isinstance(errors_raw, list), f"line {line_no}: record.errors must be a list")
    return TraceRecord(
        trace_id=obj["trace_id"],
        entry=obj["entry"],
        started_at=_num(obj.get("started_at", 0.0), "record.started_at"),
        duration_ms=_num(obj.get("duration_ms", 0.0), "record.duration_ms"),
        status=status,
        spans=[_parse_span(s, line_no) for s in spans_raw],
        errors=[_parse_error(e, line_no) for e in errors_raw],
    )


def _parse_header(line: str) -> dict:
    """解析并校验文件头；不兼容版本/格式给出明确错误。"""
    try:
        header = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"header is not valid JSON: {exc}") from None
    _require(isinstance(header, dict), "header must be an object")
    _require(header.get("format") == EXPORT_FORMAT,
             f"unrecognized file format: {header.get('format')!r}")
    version = header.get("version")
    _require(isinstance(version, int) and not isinstance(version, bool),
             f"header.version must be an integer, got {version!r}")
    _require(version in SUPPORTED_VERSIONS,
             f"unsupported content version {version}; supported: {sorted(SUPPORTED_VERSIONS)}")
    return header


def _dep_base_name(span_name: str) -> str | None:
    """从 span 名还原下游依赖名；非下游尝试（如降级本地决策）返回 None。

    命名约定为 ``dep:{dependency}.{operation}``（重试另带 ``#attemptN``），
    因此依赖名取 ``dep:`` 之后、第一个 ``.`` 之前的部分。
    """
    if not span_name.startswith("dep:"):
        return None
    if span_name.endswith(":fallback"):
        return None  # 降级本地决策不产生新的下游调用，不计数
    base = span_name.split("#attempt", 1)[0][len("dep:"):]
    return base.split(".", 1)[0]


def _apply_record(recorder, metrics, record: TraceRecord) -> bool:
    """落一条记录并补登记聚合指标；已存在则整体跳过（幂等）。"""
    if not recorder.save_if_absent(record):
        return False
    is_error = record.status != "ok"
    metrics.record_entry(record.entry, record.duration_ms, is_error=is_error)
    # 按 span 还原下游调用计数：每个 dep span 是一次物理尝试，
    # 同一 (trace, dependency) 的逻辑调用由 metrics 的去重集保证只计一次，
    # 重试/降级不会被重复计数。
    seen_bases: set[str] = set()
    for span in record.spans:
        base = _dep_base_name(span.name)
        if base is None:
            continue
        metrics.record_dependency(
            record.trace_id,
            base,
            f"imported#{record.trace_id}#{base}",
            is_error=span.status is SpanStatus.ERROR,
            is_retry=span.attempt > 1 or base in seen_bases,
        )
        seen_bases.add(base)
    return True


def import_traces(recorder, metrics, path: str) -> ImportResult:
    """从本地文件导入追踪记录。

    两遍流式读取：第一遍只校验（版本、格式、每行结构），任何损坏或
    不兼容都在此返回明确失败、环境不被触碰；第二遍逐条应用，
    已存在的 trace 整体跳过，保证幂等且不覆盖已有数据。
    """
    if not os.path.isfile(path):
        return ImportResult(ok=False, error=f"file not found: {path}")
    # 第一遍：整体校验（流式，常量内存）。
    try:
        with open(path, "r", encoding="utf-8") as fh:
            first = fh.readline()
            _require(first.strip(), "file is empty or missing header")
            _parse_header(first)
            for line_no, line in enumerate(fh, start=2):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"line {line_no}: record is not valid JSON: {exc}"
                    ) from None
                _parse_record(obj, line_no)
    except (OSError, ValueError) as exc:
        return ImportResult(ok=False, error=str(exc))
    # 第二遍：逐条应用（全部记录已通过校验，不会留下半份脏数据）。
    imported = skipped = 0
    try:
        with open(path, "r", encoding="utf-8") as fh:
            fh.readline()  # 头部已校验
            for line_no, line in enumerate(fh, start=2):
                if not line.strip():
                    continue
                record = _parse_record(json.loads(line), line_no)
                if _apply_record(recorder, metrics, record):
                    imported += 1
                else:
                    skipped += 1
    except (OSError, ValueError) as exc:  # 文件在两次读取间被改动等
        return ImportResult(ok=False, error=f"import aborted: {exc}")
    return ImportResult(ok=True, imported=imported, skipped=skipped)
