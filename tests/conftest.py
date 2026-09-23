"""Shared fixtures: clean registries per test + per-test input/attribution logs."""
from __future__ import annotations

import logging
import sys

import pytest

from app.metrics import registry
from app.services import _payment_host
from app.tracer import store

# Logs visible in pytest output with -s / --log-cli-level=INFO: each test
# prints its input, trace id and attribution conclusion.
logger = logging.getLogger("tracing-tests")


def _reset_payment_flakiness() -> None:
    for attr in list(vars(_payment_host)):
        if attr.startswith("seen_"):
            delattr(_payment_host, attr)


@pytest.fixture(autouse=True)
def _clean_state():
    registry.reset()
    store.reset()
    _reset_payment_flakiness()
    yield
    registry.reset()
    _reset_payment_flakiness()


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    import main

    return TestClient(main.app)


def pytest_configure(config: pytest.Config) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    logging.getLogger("tracing-tests").addHandler(handler)
    logging.getLogger("tracing-tests").setLevel(logging.INFO)
