"""FastAPI application entrypoint."""
from fastapi import FastAPI

from app.errors import ClassifiedError
from app.middleware import TracingMiddleware
from app.routes import (
    classified_exception_handler,
    router,
    set_body_middleware,
)

app = FastAPI(title="request-tracing-python")
# Outermost: establishes the trace around everything below.
app.add_middleware(TracingMiddleware)
# Inner: parses/caches JSON before endpoint dispatch.
app.middleware("http")(set_body_middleware)
app.add_exception_handler(ClassifiedError, classified_exception_handler)
app.include_router(router)
