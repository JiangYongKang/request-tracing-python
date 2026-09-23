"""失败归因：稳定的错误类别与可回溯的异常链。

仅定义类别与异常外壳；归因规则在实现阶段补充。
对外响应永远透出 ``TracedError.public_message``，不泄露底层库异常。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ErrorCategory(str, Enum):
    """失败的稳定归类（枚举值即对外/日志中使用的稳定标识）。"""

    VALIDATION = "validation"
    BUSINESS = "business"
    DOWNSTREAM = "downstream"
    INTERNAL = "internal"


class TracedError(Exception):
    """带归因类别与原始原因的异常。

    - ``category``: 稳定类别
    - ``public_message``: 可对外展示的脱敏信息
    - ``cause``: 原始底层异常（仅记录在本地追踪数据中，不对外透出）
    """

    def __init__(
        self,
        category: ErrorCategory,
        public_message: str,
        *,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(public_message)
        self.category = category
        self.public_message = public_message
        self.cause = cause

    def __str__(self) -> str:  # pragma: no cover - 调试辅助
        return f"[{self.category.value}] {self.public_message}"


# 未识别异常统一使用的脱敏对外文案，绝不包含底层库的原始异常文本。
_INTERNAL_PUBLIC_MESSAGE = "internal error"


def classify(exc: BaseException) -> ErrorCategory:
    """把任意异常归入稳定的 :class:`ErrorCategory`。

    业务代码必须显式抛出带类别的 :class:`TracedError`；未识别的异常
    （含所有第三方库原始异常）一律归入 ``INTERNAL``，从而保证同一种
    失败无论从哪个入口或重试路径进入，归因结果都一致。
    """
    if isinstance(exc, TracedError):
        return exc.category
    return ErrorCategory.INTERNAL


def to_public_message(exc: BaseException) -> str:
    """返回可写入对外响应的脱敏信息（绝不透出底层库原始异常文本）。"""
    if isinstance(exc, TracedError):
        return exc.public_message
    return _INTERNAL_PUBLIC_MESSAGE


def as_traced(exc: BaseException) -> TracedError:
    """把任意异常规范为 :class:`TracedError`，保留原始异常链。"""
    if isinstance(exc, TracedError):
        return exc
    return TracedError(
        ErrorCategory.INTERNAL,
        _INTERNAL_PUBLIC_MESSAGE,
        cause=exc,
    )


@dataclass
class ErrorRecord:
    """一条失败归因记录（含可回溯的原始原因，仅存本地，不对外透出）。"""

    category: str
    public_message: str
    cause_type: str | None
    cause_message: str | None
    span_name: str
    trace_id: str

    def to_dict(self) -> dict:
        return {
            "category": self.category,
            "public_message": self.public_message,
            "cause_type": self.cause_type,
            "cause_message": self.cause_message,
            "span_name": self.span_name,
            "trace_id": self.trace_id,
        }


def make_error_record(
    exc: BaseException,
    *,
    span_name: str,
    trace_id: str,
) -> ErrorRecord:
    """从任意异常生成归因记录：类别稳定、原文脱敏、原因链保留。"""
    traced = as_traced(exc)
    cause = traced.cause
    return ErrorRecord(
        category=traced.category.value,
        public_message=traced.public_message,
        cause_type=type(cause).__name__ if cause is not None else None,
        cause_message=str(cause) if cause is not None else None,
        span_name=span_name,
        trace_id=trace_id,
    )
