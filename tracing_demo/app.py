"""FastAPI 集成：追踪中间件、统一错误响应、本地查看端点。"""
from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="request-tracing-demo", version="0.1.0")
