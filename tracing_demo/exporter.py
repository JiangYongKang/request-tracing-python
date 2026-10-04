"""追踪数据导出：按条件筛选并写出独立的本地文件（骨架）。

文件格式（NDJSON，逐行 JSON，便于流式读写、内存有界）：

- 第 1 行：头部 ``{"format": ..., "version": ..., "exported_at": ..., "filters": {...}}``
- 后续每行：一条完整的 trace 记录（含 spans 与 errors，即含原始原因）。

导出在服务正常并发接待时进行：对每条记录在其 ``span_lock`` 下取快照，
保证单条链路内部一致（不会出现半条链路、父子关系或归因对不上）。
"""
from __future__ import annotations

import contextlib
import json
import os
import tempfile
import time
from dataclasses import dataclass, field

from tracing_demo.recorder import TraceRecord

#: 当前写出的内容版本。
EXPORT_VERSION = 1
#: 文件头中的格式标识。
EXPORT_FORMAT = "request-tracing-demo/export"


@dataclass(frozen=True)
class ExportFilter:
    """导出筛选条件。全部可选，缺省表示不限制。"""

    entry: str | None = None
    since: float | None = None  # started_at 下界（含），epoch 秒
    until: float | None = None  # started_at 上界（含），epoch 秒
    failed_only: bool = False
    error_category: str | None = None


@dataclass
class ExportResult:
    ok: bool
    path: str = ""
    record_count: int = 0
    error: str | None = None


def _matches(record: TraceRecord, filters: ExportFilter) -> bool:
    """单条记录是否满足筛选条件（在记录快照上判断，结果稳定）。"""
    snap = record.to_dict()
    if filters.entry is not None and snap["entry"] != filters.entry:
        return False
    if filters.since is not None and snap["started_at"] < filters.since:
        return False
    if filters.until is not None and snap["started_at"] > filters.until:
        return False
    if filters.failed_only and snap["status"] == "ok":
        return False
    if filters.error_category is not None:
        categories = {e["category"] for e in snap["errors"]}
        if filters.error_category not in categories:
            return False
    return True


def export_traces(
    recorder, path: str, filters: ExportFilter | None = None
) -> ExportResult:
    """把满足条件的追踪记录以一致快照写入本地文件（NDJSON）。

    - 快照：先取 recorder 的记录清单快照，再逐条在记录自身的
      ``span_lock`` 下序列化；导出期间并发写入/后台追加不会导致
      单条链路只导出一半或父子、归因对不上。
    - 流式写出：逐条序列化逐行写，文件大也不会占用超出单条记录的内存。
    - 原子落盘：先写临时文件再 ``os.replace``，失败不留半截文件。
    """
    filters = filters or ExportFilter()
    records = recorder.all()  # 记录清单快照（引用副本）
    try:
        out_dir = os.path.dirname(os.path.abspath(path))
        os.makedirs(out_dir, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=out_dir, suffix=".tmp")
        count = 0
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                header = {
                    "format": EXPORT_FORMAT,
                    "version": EXPORT_VERSION,
                    "exported_at": time.time(),
                    "filters": {
                        "entry": filters.entry,
                        "since": filters.since,
                        "until": filters.until,
                        "failed_only": filters.failed_only,
                        "error_category": filters.error_category,
                    },
                }
                fh.write(json.dumps(header, ensure_ascii=False) + "\n")
                for record in records:
                    if not _matches(record, filters):
                        continue
                    # to_dict 在记录锁内取 spans+errors 一致快照。
                    fh.write(
                        json.dumps(record.to_dict(), ensure_ascii=False) + "\n"
                    )
                    count += 1
            os.replace(tmp_path, path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise
        return ExportResult(ok=True, path=path, record_count=count)
    except OSError as exc:
        return ExportResult(ok=False, path=path, error=f"export failed: {exc}")
