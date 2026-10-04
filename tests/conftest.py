"""测试公共装置：每个用例前后复位进程内全局单例，保证相互隔离。"""
from __future__ import annotations

import pytest

from tracing_demo.metrics import reset_metrics
from tracing_demo.recorder import reset_recorder
from tracing_demo.service import reset_background


@pytest.fixture(autouse=True)
def _clean_globals():
    reset_recorder()
    reset_metrics()
    reset_background()
    yield
    reset_recorder()
    reset_metrics()
    reset_background()
