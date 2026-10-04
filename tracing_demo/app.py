"""FastAPI 集成：追踪中间件、统一错误响应、本地查看与导出/导入端点。

- 每个业务入口请求开启独立 trace，异常在边界统一归类、脱敏后返回；
- 请求结束时把完整 TraceRecord 写入本地 recorder（与后台任务共享
  上下文锁，后台继续写入时读取仍一致），并登记入口聚合指标；
- 管理端点（/traces、/metrics、/export、/import）不参与追踪，避免自噪声。
"""
from __future__ import annotations

import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from tracing_demo import service
from tracing_demo.context import detach_trace, record_error, start_trace, use_span
from tracing_demo.errors import TracedError, as_traced
from tracing_demo.metrics import get_metrics
from tracing_demo.recorder import TraceRecord, get_recorder
from tracing_demo.transfer import ExportFilter, export_traces, import_traces

app = FastAPI(title="request-tracing-demo", version="0.2.0")

_MANAGEMENT_PREFIXES = ("/traces", "/metrics", "/export", "/import", "/openapi", "/docs")


@app.middleware("http")
async def tracing_middleware(request: Request, call_next):
    entry = request.url.path
    if entry.startswith(_MANAGEMENT_PREFIXES):
        return await call_next(request)

    ctx, token = start_trace(entry)
    started_wall = time.time()
    started_perf = time.perf_counter()
    status = "ok"
    try:
        try:
            with use_span(f"entry:{entry}"):
                response = await call_next(request)
        except TracedError as exc:
            # 业务/校验/下游失败：类别稳定，对外只给脱敏信息。
            record_error(exc, span_name=f"entry:{entry}")
            status = "error"
            response = JSONResponse(
                status_code=400 if exc.category.value in ("validation", "business") else 502,
                content={
                    "error": exc.public_message,
                    "category": exc.category.value,
                    "trace_id": ctx.trace_id,
                },
            )
        except Exception as exc:
            # 未识别异常：统一归 INTERNAL，绝不透出底层库原文。
            traced = as_traced(exc)
            record_error(traced, span_name=f"entry:{entry}")
            status = "error"
            response = JSONResponse(
                status_code=500,
                content={
                    "error": traced.public_message,
                    "category": traced.category.value,
                    "trace_id": ctx.trace_id,
                },
            )
        if response.status_code >= 500:
            status = "error"
        return response
    finally:
        duration_ms = (time.perf_counter() - started_perf) * 1000.0
        # 记录与 TraceContext 共享 spans/errors 与锁：后台任务之后继续
        # 追加 span 时，读取侧（查看/导出）仍在同一把锁内拿到一致快照。
        get_recorder().save(
            TraceRecord(
                trace_id=ctx.trace_id,
                entry=entry,
                started_at=started_wall,
                duration_ms=duration_ms,
                status=status,
                spans=ctx.spans,
                errors=ctx.errors,
                span_lock=ctx.lock,
            )
        )
        get_metrics().record_entry(entry, duration_ms, is_error=(status == "error"))
        detach_trace(token)


# ---------------------------------------------------------------------------
# 业务入口
# ---------------------------------------------------------------------------
@app.get("/status")
async def status():
    return service.get_status()


@app.post("/orders")
async def orders(payload: dict):
    return await service.submit_order(payload)


@app.post("/payments")
async def payments(payload: dict):
    return await service.submit_payment(payload)


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
async def metrics():
    return get_metrics().snapshot()


# ---------------------------------------------------------------------------
# 导出 / 导入（本地文件）
# ---------------------------------------------------------------------------
@app.post("/export")
async def export(payload: dict):
    filt = ExportFilter(
        entry=payload.get("entry"),
        started_after=payload.get("started_after"),
        started_before=payload.get("started_before"),
        only_failed=bool(payload.get("only_failed", False)),
        error_category=payload.get("error_category"),
    )
    path = payload.get("path")
    if not path:
        return JSONResponse(status_code=400, content={"error": "field 'path' is required"})
    result = export_traces(get_recorder(), path, filt)
    code = 200 if result.ok else 500
    return JSONResponse(status_code=code, content=result.__dict__)


@app.post("/import")
async def import_(payload: dict):
    path = payload.get("path")
    if not path:
        return JSONResponse(status_code=400, content={"error": "field 'path' is required"})
    result = import_traces(get_recorder(), get_metrics(), path)
    code = 200 if result.ok else 400
    return JSONResponse(status_code=code, content=result.__dict__)
