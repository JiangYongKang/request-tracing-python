"""FastAPI application entrypoint."""
from fastapi import FastAPI

from app.middleware import TracingMiddleware
from app.routes import router

app = FastAPI(title="request-tracing-python")
app.add_middleware(TracingMiddleware)
app.include_router(router)
