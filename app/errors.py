"""Failure categories, wrapping exception and consistent classification."""
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

    def __str__(self) -> str:  # pragma: no cover - trivial
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


def business_error(message: str, *, cause: BaseException | None = None) -> ClassifiedError:
    raise NotImplementedError


def validation_error(message: str, *, cause: BaseException | None = None) -> ClassifiedError:
    raise NotImplementedError


def downstream_error(message: str, *, cause: BaseException | None = None) -> ClassifiedError:
    raise NotImplementedError


def internal_error(message: str, *, cause: BaseException | None = None) -> ClassifiedError:
    raise NotImplementedError


def classify(exc: BaseException) -> ClassifiedError:
    """Map any exception to exactly one failure category, deterministically."""
    raise NotImplementedError


def public_payload(exc: ClassifiedError) -> dict:
    raise NotImplementedError
