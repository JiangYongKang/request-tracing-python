"""测试公共设施：状态复位与日志（日志需能看出输入、追踪标识、归因结论）。"""
from __future__ import annotations

import logging

import pytest

from tracing_demo.metrics import reset_metrics
from tracing_demo.recorder import reset_recorder
from tracing_demo.service import reset_background

log = logging.getLogger("tracing-test")


@pytest.fixture(autouse=True)
def clean_state():
    """每个用例前后复位进程内单例，保证环境干净、互不影响。"""
    reset_recorder()
    reset_metrics()
    reset_background()
    yield
    reset_recorder()
    reset_metrics()
    reset_background()
