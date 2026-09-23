"""Tests: concurrent isolation, failure chains, retry accounting, bounds."""
from __future__ import annotations


def test_skeleton_imports() -> None:
    import main  # noqa: F401
    from app import context, errors, metrics, tracer  # noqa: F401

    assert main.app is not None
