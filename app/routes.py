"""HTTP endpoints demonstrating trace propagation across execution paths."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .context import (
    begin_background,
    current_context,
    end_background,
    report_error,
    run_with_context,
)
from .errors import (
    ClassifiedError,
    public_payload,
    validation_error,
)
from .metrics import registry
from .services import place_order, place_order_async
from .tracer import span_scope, store

router = APIRouter()

# Bounded worker pool for background/threaded work demonstrating trace
# propagation across threads without leaking context between requests.
_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="bg-task")


def _trace_id() -> str | None:
    ctx = current_context()
    return ctx.trace_id if ctx is not None else None


@router.get("/")
def read_root() -> dict:
    return {"hello": "world", "trace_id": _trace_id()}


@router.post("/orders")
def create_order(request: Request) -> dict:
    """Synchronous entry: validate -> sync downstreams, with retries."""
    payload = request.state.body
    if payload is None:
        raise validation_error("request body must be a JSON object")
    result = place_order(payload)
    result["trace_id"] = _trace_id()
    return result


@router.post("/orders/async")
async def create_order_async(request: Request) -> dict:
    """Async entry: interleavable async downstreams."""
    payload = request.state.body
    if payload is None:
        raise validation_error("request body must be a JSON object")
    result = await place_order_async(payload)
    result["trace_id"] = _trace_id()
    return result


@router.post("/orders/background")
async def create_order_background(request: Request) -> dict:
    """Entry whose heavy work runs in a worker thread + a delayed background
    step, both continuing the *same* trace via explicit context propagation."""
    payload = request.state.body
    if payload is None:
        raise validation_error("request body must be a JSON object")
    ctx = current_context()
    assert ctx is not None

    def threaded_step() -> dict:
        # Runs on another thread with a fresh context copy bound to this trace.
        return place_order(payload)

    with span_scope("threadpool_dispatch"):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            _executor,
            lambda: run_with_context(ctx, threaded_step),
        )

    async def background_step() -> None:
        # asyncio copies the current context when the task is created, so this
        # task keeps the same trace even if it finishes after the response.
        try:
            with span_scope("post_response_hook"):
                await asyncio.sleep(0.01)
        finally:
            end_background()

    begin_background()
    request.state.background_task = asyncio.create_task(background_step())
    result["trace_id"] = ctx.trace_id
    result["mode"] = result.get("mode", "sync") + "+background"
    return result


@router.get("/metrics")
def get_metrics() -> dict:
    return registry.snapshot()


@router.get("/traces")
def get_traces(trace_id: str | None = None) -> dict:
    if trace_id is not None:
        record = store.get(trace_id)
        return {"trace": record}
    return {"store": store.status(), "traces": store.all()}


async def set_body_middleware(request: Request, call_next):
    """Tiny middleware: cache raw JSON body so endpoints can parse it and so
    malformed JSON yields our validation category instead of a framework 500."""
    if request.method == "POST":
        try:
            request.state.body = await request.json()
        except Exception:  # noqa: BLE001
            request.state.body = None
    return await call_next(request)


async def classified_exception_handler(
    request: Request, exc: ClassifiedError
) -> JSONResponse:
    """Uniform public error shape; raw cause never serialized here.

    Publishing the error lets the outer middleware attribute the trace and
    metrics consistently even though the exception never propagates to it.
    """
    report_error(exc)
    return JSONResponse(
        status_code=exc.http_status,
        content=public_payload(exc),
    )
