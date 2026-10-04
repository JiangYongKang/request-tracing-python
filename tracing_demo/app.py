"""FastAPI 集成：追踪中间件、统一错误响应、本地查看与导出/导入端点。"""
from __future__ import annotations

import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from tracing_demo import service
from tracing_demo.context import detach_trace, record_error, start_trace, use_span
from tracing_demo.errors import ErrorCategory, TracedError, to_public_message
from tracing_demo.exporter import ExportFilter, export_traces
from tracing_demo.importer import import_traces
from tracing_demo.metrics import get_metrics
from tracing_demo.recorder import TraceRecord, get_recorder

app = FastAPI(title="request-tracing-demo", version="0.2.0")

#: 查看/管理类端点自身不产生追踪记录，避免污染数据。
_SKIP_TRACE_PREFIXES = ("/traces", "/metrics")

_CATEGORY_STATUS = {
    ErrorCategory.VALIDATION: 400,
    ErrorCategory.BUSINESS: 409,
    ErrorCategory.DOWNSTREAM: 502,
    ErrorCategory.INTERNAL: 500,
}


@app.middleware("http")
async def tracing_middleware(request: Request, call_next):
    path = request.url.path
    if path.startswith(_SKIP_TRACE_PREFIXES):
        return await call_next(request)

    entry = f"{request.method} {path}"
    ctx, token = start_trace(entry)
    started = time.perf_counter()
    status = "ok"
    try:
        with use_span(f"entry:{entry}") as root_span:
            span_name = root_span.name
            try:
                response = await call_next(request)
                if response.status_code >= 500:
                    status = "error"
            except TracedError as exc:
                status = "error"
                record_error(exc, span_name=span_name)
                response = JSONResponse(
                    status_code=_CATEGORY_STATUS[exc.category],
                    content={"error": {"category": exc.category.value,
                                       "message": exc.public_message}},
                )
            except Exception as exc:  # 未识别异常 -> INTERNAL，脱敏对外
                status = "error"
                record_error(exc, span_name=span_name)
                response = JSONResponse(
                    status_code=500,
                    content={"error": {"category": ErrorCategory.INTERNAL.value,
                                       "message": to_public_message(exc)}},
                )
        return response
    finally:
        duration_ms = (time.perf_counter() - started) * 1000.0
        get_metrics().record_entry(entry, duration_ms, is_error=status == "error")
        # 记录与活跃上下文共享 spans/errors 列表与同一把锁：
        # 后台任务之后追加的 span 仍会进入该记录，且读取始终一致。
        get_recorder().save(TraceRecord(
            trace_id=ctx.trace_id,
            entry=entry,
            started_at=time.time() - duration_ms / 1000.0,
            duration_ms=duration_ms,
            status=status,
            spans=ctx.spans,
            errors=ctx.errors,
            span_lock=ctx.lock,
        ))
        detach_trace(token)


# ---------------------------------------------------------------------------
# 业务入口
# ---------------------------------------------------------------------------
@app.post("/orders")
async def post_orders(payload: dict):
    return await service.submit_order(payload)


@app.post("/payments")
async def post_payments(payload: dict):
    return await service.submit_payment(payload)


@app.get("/status")
async def get_status():
    return service.get_status()


# ---------------------------------------------------------------------------
# 本地查看
# ---------------------------------------------------------------------------
@app.get("/traces")
async def list_traces():
    return {
        "traces": [
            {
                "trace_id": r.trace_id,
                "entry": r.entry,
                "started_at": r.started_at,
                "duration_ms": r.duration_ms,
                "status": r.status,
                "error_categories": sorted({e["category"] for e in r.to_dict()["errors"]}),
            }
            for r in get_recorder().all()
        ]
    }


@app.get("/traces/{trace_id}")
async def get_trace(trace_id: str):
    record = get_recorder().get(trace_id)
    if record is None:
        return JSONResponse(status_code=404, content={"error": "trace not found"})
    return record.to_dict()


@app.get("/metrics")
async def get_metrics_snapshot():
    return get_metrics().snapshot()


# ---------------------------------------------------------------------------
# 导出 / 导入（本地文件）
# ---------------------------------------------------------------------------
@app.post("/traces/export")
async def post_export(payload: dict):
    filters = ExportFilter(
        entry=payload.get("entry"),
        since=payload.get("since"),
        until=payload.get("until"),
        failed_only=bool(payload.get("failed_only", False)),
        error_category=payload.get("error_category"),
    )
    result = export_traces(get_recorder(), payload["path"], filters)
    status_code = 200 if result.ok else 400
    return JSONResponse(status_code=status_code, content=vars(result))


@app.post("/traces/import")
async def post_import(payload: dict):
    result = import_traces(get_recorder(), get_metrics(), payload["path"])
    status_code = 200 if result.ok else 400
    return JSONResponse(status_code=status_code, content=vars(result))
