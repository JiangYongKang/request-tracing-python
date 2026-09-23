"""ASGI middleware: one trace per entry request, metrics, safe error body.

Responsibilities
----------------
* open a fresh trace before the application runs and bind it to the request's
  execution context. ``asyncio`` tasks (endpoint, background tasks spawned via
  ``run_in_threadpool`` / ``BackgroundTasks``) inherit the same context, while
  each request gets its own trace — no cross-request leakage possible;
* rename the entry from the raw path to the matched route template once
  routing has resolved (stable aggregation key, no high-cardinality ids);
* classify any escaping exception into one failure category, return a public
  payload that never exposes the underlying library exception, and persist
  the full trace (with cause chain) to the local trace store;
* record exactly one aggregate sample per logical request; retry/fallback
  usage rides along as attributes instead of extra calls.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .context import (
    enable_completion,
    reported_error,
    set_entry_name,
)
from .errors import ClassifiedError, classify, public_payload
from .metrics import registry
from .tracer import build_trace_record, store

_TRACE_HEADER = "x-trace-id"
#: Upper bound on how long trace finalization waits for post-response work,
#: keeping request teardown bounded even if a background task hangs.
BACKGROUND_JOIN_TIMEOUT_S = 2.0


def _route_template(scope: Scope) -> str | None:
    root_path = scope.get("root_path", "")
    route = scope.get("route")
    path_format = getattr(route, "path_format", None)
    if path_format:
        return root_path + path_format
    return None


class TracingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET")
        raw_path = scope.get("path", "/")
        default_entry = f"{method} {raw_path}"

        from .tracer import trace_scope

        with trace_scope(default_entry) as ctx:
            enable_completion(ctx)
            status_holder: dict[str, int] = {"code": 500}

            async def send_wrapper(message: Message) -> None:
                if message["type"] == "http.response.start":
                    status_holder["code"] = message.get("status", 500)
                    headers = message.setdefault("headers", [])
                    # Exactly one trace header (an inner handler may already
                    # have produced one).
                    if not any(
                        name.lower() == _TRACE_HEADER.encode()
                        for name, _ in headers
                    ):
                        headers.append(
                            (_TRACE_HEADER.encode(), ctx.trace_id.encode())
                        )
                await send(message)

            try:
                await self.app(scope, receive, send_wrapper)
            except BaseException as exc:  # noqa: BLE001 - total classification
                await self._await_background(ctx)
                err = classify(exc)
                record = build_trace_record(ctx, err)
                self._record_metrics(record)
                store.save(record)
                await self._send_error(send, err, ctx.trace_id)
                return

            # Routing has resolved by now: use the stable route template.
            template = _route_template(scope)
            if template is not None:
                entry = f"{method} {template}"
                set_entry_name(entry)

            # Let post-response background work finish so its spans are part
            # of this trace (bounded wait to keep teardown deterministic).
            await self._await_background(ctx)

            # An inner exception handler may have converted an error into a
            # normal response; attribute the trace accordingly.
            handled_err = reported_error()
            record = build_trace_record(ctx, handled_err)
            if (
                record["error_category"] is None
                and status_holder["code"] >= 500
            ):
                # Truly unhandled server errors map to internal_error.
                record["status"] = "error"
                record["error_category"] = "internal_error"
            self._record_metrics(record)
            store.save(record)

    @staticmethod
    async def _await_background(ctx: Any) -> None:
        if ctx.completion is not None and ctx.pending > 0:
            try:
                await asyncio.wait_for(
                    ctx.completion.wait(), timeout=BACKGROUND_JOIN_TIMEOUT_S
                )
            except TimeoutError:
                pass

    @staticmethod
    def _record_metrics(record: dict[str, Any]) -> None:
        registry.record(
            record["entry"] or "unknown",
            record.get("duration_ms") or 0.0,
            error_category=record["error_category"],
            retry_attempts=record.get("retry_attempts", 0),
            fell_back=record.get("fell_back", False),
        )

    @staticmethod
    async def _send_error(
        send: Send, err: ClassifiedError, trace_id: str
    ) -> None:
        body = json.dumps(public_payload(err)).encode()
        headers = [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
            (_TRACE_HEADER.encode(), trace_id.encode()),
        ]
        await send(
            {
                "type": "http.response.start",
                "status": err.http_status,
                "headers": headers,
            }
        )
        await send({"type": "http.response.body", "body": body})
