"""ASGI middleware that opens a trace per entry request and records metrics."""
from __future__ import annotations

from typing import Any

from starlette.types import ASGIApp, Receive, Scope, Send

from .context import _current, current_context
from .errors import classify
from .metrics import registry
from .tracer import build_trace_record, store


class TracingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        raise NotImplementedError
