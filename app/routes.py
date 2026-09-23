"""HTTP endpoints demonstrating trace propagation across paths."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Request

from .context import current_context, run_with_context
from .errors import classify
from .metrics import registry
from .services import (
    place_order,
    place_order_async,
)
from .tracer import span_scope, store

router = APIRouter()

_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="bg-task")


@router.get("/")
def read_root() -> dict:
    raise NotImplementedError


@router.post("/orders")
def create_order(request: Request) -> dict:
    raise NotImplementedError


@router.post("/orders/async")
async def create_order_async(request: Request) -> dict:
    raise NotImplementedError


@router.get("/metrics")
def get_metrics() -> dict:
    raise NotImplementedError


@router.get("/traces")
def get_traces(trace_id: str | None = None) -> dict:
    raise NotImplementedError
