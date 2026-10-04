"""追踪数据的本地导出 / 导入（可携带的快照文件）。

文件为 JSON Lines 格式，便于流式读写、避免一次性占满内存：

- 第 1 行：头部（格式名、内容版本、导出时间、筛选条件）；
- 中间每行一条完整 trace 记录（含 spans 与失败归因，链路级自洽）；
- 最后 1 行：尾部（记录条数与内容校验和），用于发现损坏/不完整文件。

版本兼容：``CURRENT_VERSION`` 为当前写入版本；``COMPATIBLE_VERSIONS``
中列出的旧版本在导入时做迁移还原，其余版本明确拒绝。
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field

FORMAT_NAME = "trace-export"
CURRENT_VERSION = 2
# 可兼容导入的旧内容版本。v1 的 span 没有 attempt 字段，导入时补默认值。
COMPATIBLE_VERSIONS = {1, 2}


@dataclass
class ExportFilter:
    """导出筛选条件：按入口、时间范围、是否失败、失败类别。"""

    entry: str | None = None
    started_after: float | None = None  # epoch 秒，含边界
    started_before: float | None = None  # epoch 秒，含边界
    only_failed: bool = False
    error_category: str | None = None

    def to_dict(self) -> dict:
        return {
            "entry": self.entry,
            "started_after": self.started_after,
            "started_before": self.started_before,
            "only_failed": self.only_failed,
            "error_category": self.error_category,
        }


@dataclass
class ExportResult:
    ok: bool
    path: str = ""
    record_count: int = 0
    error: str | None = None


@dataclass
class ImportResult:
    ok: bool
    added: int = 0
    skipped_existing: int = 0
    evicted: int = 0
    error: str | None = None


def _matches(record: dict, filt: ExportFilter) -> bool:
    """判断一条已快照的 trace 记录是否满足导出筛选条件。"""
    if filt.entry is not None and record.get("entry") != filt.entry:
        return False
    started = record.get("started_at")
    if filt.started_after is not None and (started is None or started < filt.started_after):
        return False
    if filt.started_before is not None and (started is None or started > filt.started_before):
        return False
    if filt.only_failed and record.get("status") != "error":
        return False
    if filt.error_category is not None:
        categories = {e.get("category") for e in record.get("errors", [])}
        categories.update(
            s.get("error_category") for s in record.get("spans", []) if s.get("error_category")
        )
        if filt.error_category not in categories:
            return False
    return True


def export_traces(recorder, path: str, filt: ExportFilter | None = None) -> ExportResult:
    """把满足条件的 trace 以一致快照导出到本地文件（流式写入）。

    一致性保证：先取记录列表快照（同一时间点上的集合），每条记录再
    通过各自 ``span_lock`` 加锁序列化，因此并发请求与后台任务继续写
    同一条链路时，导出的每条链路都是完整自洽的（spans 与失败归因
    来自同一时刻），不会出现半条链路。
    """
    filt = filt or ExportFilter()
    records = recorder.all()  # 记录集合的点时快照
    count = 0
    hasher = hashlib.sha256()
    try:
        with open(path, "w", encoding="utf-8") as fh:
            header = {
                "format": FORMAT_NAME,
                "version": CURRENT_VERSION,
                "created_at": time.time(),
                "filter": filt.to_dict(),
            }
            fh.write(json.dumps(header, ensure_ascii=False) + "\n")
            for rec in records:
                data = rec.to_dict()  # 单条链路在锁内快照，父子/归因自洽
                if not _matches(data, filt):
                    continue
                line = json.dumps({"type": "record", "record": data}, ensure_ascii=False)
                hasher.update(line.encode("utf-8"))
                fh.write(line + "\n")
                count += 1
            footer = {
                "type": "footer",
                "record_count": count,
                "sha256": hasher.hexdigest(),
            }
            fh.write(json.dumps(footer, ensure_ascii=False) + "\n")
    except OSError as exc:
        return ExportResult(ok=False, path=path, error=f"export failed: {exc}")
    return ExportResult(ok=True, path=path, record_count=count)


def import_traces(recorder, metrics, path: str) -> ImportResult:
    """从本地文件导入 trace：幂等、原子（失败不留半份数据）、受容量上界约束。

    两遍流式处理，避免大文件一次性占满内存：

    1. 校验遍：逐行检查头部格式/版本、每条记录结构、尾部条数与校验和；
       任一失败都直接返回错误结果，此时环境未被改动；
    2. 应用遍：逐条重建记录写入 ``recorder``（``save_if_absent`` 保证
       重复导入幂等、不覆盖已有数据），并为真正新增的记录补登入口聚合
       指标；容量超出上界时由 recorder 按既有 FIFO 策略淘汰。
    """
    # ---- 第一遍：流式校验，不在环境中留下任何改动 ----
    try:
        with open(path, "r", encoding="utf-8") as fh:
            header_line = fh.readline()
            if not header_line:
                return ImportResult(ok=False, error="empty file")
            try:
                header = json.loads(header_line)
            except json.JSONDecodeError:
                return ImportResult(ok=False, error="corrupt header: not valid JSON")
            if not isinstance(header, dict) or header.get("format") != FORMAT_NAME:
                return ImportResult(ok=False, error="not a trace export file")
            version = header.get("version")
            if version not in COMPATIBLE_VERSIONS:
                return ImportResult(
                    ok=False,
                    error=f"unsupported content version: {version!r} "
                    f"(compatible: {sorted(COMPATIBLE_VERSIONS)})",
                )

            hasher = hashlib.sha256()
            record_lines = 0
            footer: dict | None = None
            for raw in fh:
                raw = raw.rstrip("\n")
                if not raw:
                    continue
                try:
                    item = json.loads(raw)
                except json.JSONDecodeError:
                    return ImportResult(ok=False, error=f"corrupt record line {record_lines + 1}")
                if not isinstance(item, dict) or "type" not in item:
                    return ImportResult(ok=False, error=f"corrupt line {record_lines + 1}")
                if item["type"] == "footer":
                    footer = item
                    break
                if item["type"] != "record" or not isinstance(item.get("record"), dict):
                    return ImportResult(ok=False, error=f"corrupt record line {record_lines + 1}")
                if not isinstance(item["record"].get("trace_id"), str):
                    return ImportResult(ok=False, error=f"record line {record_lines + 1} missing trace_id")
                hasher.update(raw.encode("utf-8"))
                record_lines += 1
            if footer is None:
                return ImportResult(ok=False, error="incomplete file: missing footer")
            if footer.get("record_count") != record_lines or footer.get("sha256") != hasher.hexdigest():
                return ImportResult(ok=False, error="corrupt file: checksum or count mismatch")
    except OSError as exc:
        return ImportResult(ok=False, error=f"import failed: {exc}")

    # ---- 第二遍：逐条应用（数据已整体校验通过） ----
    added = 0
    skipped = 0
    before = len(recorder)
    with open(path, "r", encoding="utf-8") as fh:
        fh.readline()  # 跳过头部
        for raw in fh:
            item = json.loads(raw)
            if item.get("type") != "record":
                break
            record = _record_from_dict(item["record"], version=version)
            if recorder.save_if_absent(record):
                added += 1
                if metrics is not None:
                    metrics.record_entry(
                        record.entry,
                        record.duration_ms,
                        is_error=record.status == "error",
                    )
            else:
                skipped += 1
    evicted = max(0, before + added - len(recorder))
    return ImportResult(ok=True, added=added, skipped_existing=skipped, evicted=evicted)


def _record_from_dict(data: dict, *, version: int):
    """把文件中的记录字典还原为 :class:`TraceRecord`，按版本做兼容迁移。"""
    from tracing_demo.context import Span, SpanStatus
    from tracing_demo.errors import ErrorRecord
    from tracing_demo.recorder import TraceRecord

    spans = []
    for s in data.get("spans", []):
        started = float(s.get("started_at", 0.0))
        duration_ms = float(s.get("duration_ms", 0.0))
        spans.append(
            Span(
                span_id=s["span_id"],
                parent_id=s.get("parent_id"),
                name=s["name"],
                started_at=started,
                ended_at=started + duration_ms / 1000.0,
                status=SpanStatus(s.get("status", "ok")),
                error_category=s.get("error_category"),
                # v1 内容没有 attempt 字段，迁移时补默认值。
                attempt=int(s.get("attempt", 1)) if version >= 2 else 1,
            )
        )
    errors = [
        ErrorRecord(
            category=e["category"],
            public_message=e.get("public_message", ""),
            cause_type=e.get("cause_type"),
            cause_message=e.get("cause_message"),
            span_name=e.get("span_name", ""),
            trace_id=e.get("trace_id", data["trace_id"]),
        )
        for e in data.get("errors", [])
    ]
    return TraceRecord(
        trace_id=data["trace_id"],
        entry=data.get("entry", ""),
        started_at=float(data.get("started_at", 0.0)),
        duration_ms=float(data.get("duration_ms", 0.0)),
        status=data.get("status", "ok"),
        spans=spans,
        errors=errors,
    )
