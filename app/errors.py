"""Failure categories, wrapping exception and consistent classification.

Four mutually exclusive categories
----------------------------------
``business_error``      request semantically rejected by business rules
``validation_error``    malformed parameters / payloads
``downstream_error``    a dependency (sync/async, simulated network, DB) failed
``internal_error``      unexpected failure; the catch-all safety net

Determinism
-----------
:func:`classify` is total: *any* exception maps to exactly one category, and
the mapping depends only on the exception itself. The same failure arriving
via a different entrypoint or surfacing after a retry is therefore attributed
identically. Downstream library exceptions (``ConnectionError``, ``TimeoutError``,
``OSError`` ...) are wrapped and never leak raw through the public API; their
original type/message are retained on ``cause`` and recorded on the error span.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FailureCategory(str, Enum):
    BUSINESS = "business_error"
    VALIDATION = "validation_error"
    DOWNSTREAM = "downstream_error"
    INTERNAL = "internal_error"


@dataclass
class ClassifiedError(Exception):
    category: FailureCategory
    message: str
    cause: BaseException | None = None
    public_message: str = "request failed"
    http_status: int = 500

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.category.value}: {self.message}"


_PUBLIC_MESSAGES: dict[FailureCategory, str] = {
    FailureCategory.BUSINESS: "business rule rejected",
    FailureCategory.VALIDATION: "invalid request parameters",
    FailureCategory.DOWNSTREAM: "dependent service unavailable",
    FailureCategory.INTERNAL: "internal server error",
}

_STATUS: dict[FailureCategory, int] = {
    FailureCategory.BUSINESS: 422,
    FailureCategory.VALIDATION: 400,
    FailureCategory.DOWNSTREAM: 502,
    FailureCategory.INTERNAL: 500,
}

#: Raw standard-library exception types that originate from downstream I/O and
#: must never be surfaced verbatim to callers.
_DOWNSTREAM_EXC_TYPES: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
)


def _make(
    category: FailureCategory, message: str, cause: BaseException | None
) -> ClassifiedError:
    err = ClassifiedError(
        category=category,
        message=message,
        cause=cause,
        public_message=_PUBLIC_MESSAGES[category],
        http_status=_STATUS[category],
    )
    if cause is not None:
        # Standard exception chaining; trace spans also capture cause explicitly.
        err.__cause__ = cause
    return err


def business_error(
    message: str, *, cause: BaseException | None = None
) -> ClassifiedError:
    return _make(FailureCategory.BUSINESS, message, cause)


def validation_error(
    message: str, *, cause: BaseException | None = None) -> ClassifiedError:
    return _make(FailureCategory.VALIDATION, message, cause)


def downstream_error(
    message: str, *, cause: BaseException | None = None
) -> ClassifiedError:
    return _make(FailureCategory.DOWNSTREAM, message, cause)


def internal_error(
    message: str, *, cause: BaseException | None = None
) -> ClassifiedError:
    return _make(FailureCategory.INTERNAL, message, cause)


def classify(exc: BaseException) -> ClassifiedError:
    """Map any exception to exactly one :class:`ClassifiedError`.

    * :class:`ClassifiedError` passes through unchanged (idempotent) so that
      re-entry through another endpoint or a retry path keeps the attribution.
    * Raw downstream I/O errors are always wrapped as ``downstream_error``.
    * :class:`ValueError` / :class:`TypeError` / :class:`KeyError` raised by
      malformed input are treated as validation failures.
    * Anything else is a non-committal ``internal_error``.
    """
    if isinstance(exc, ClassifiedError):
        return exc
    if isinstance(exc, _DOWNSTREAM_EXC_TYPES):
        return _make(
            FailureCategory.DOWNSTREAM,
            f"downstream failure: {type(exc).__name__}",
            exc,
        )
    if isinstance(exc, (ValueError, TypeError, KeyError)):
        return _make(
            FailureCategory.VALIDATION,
            f"invalid parameter: {type(exc).__name__}",
            exc,
        )
    return _make(
        FailureCategory.INTERNAL,
        f"unexpected error: {type(exc).__name__}",
        exc,
    )


def public_payload(err: ClassifiedError) -> dict:
    """Safe public error body — never includes library messages or types."""
    return {
        "error": {
            "category": err.category.value,
            "message": err.public_message,
        }
    }
