"""服务入口：``uvicorn main:app`` 启动本地请求追踪演示服务。"""
from tracing_demo.app import app

__all__ = ["app"]
