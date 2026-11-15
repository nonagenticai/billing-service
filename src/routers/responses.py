"""Canonical fleet error-envelope definitions for metering_service.

`Envelope[T]` (in `models/domain.py`) is the **success** response wrapper —
preserved verbatim. This module adds the **error** half of the standard:
`ErrorDetail` + `ErrorResponse(error: ErrorDetail)`, matching the same
shape used by `nlp_service`, `worker_management`, and the rest of the
fleet. Exception handlers in `src/main.py` emit responses using these
models so `n8n custom-node generators / ultrareview` see the same error
shape coming out of every internal service.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ErrorDetail(BaseModel):
    """Inner error payload."""

    code: str = Field(..., description="Stable machine-readable error code")
    message: str = Field(..., description="Human-readable error description")
    status: int = Field(..., description="HTTP status code")
    request_id: str | None = Field(default=None, description="Request UUID7 for log correlation")
    timestamp: str | None = Field(
        default=None, description="ISO-8601 UTC timestamp the error was emitted"
    )
    details: str | None = Field(
        default=None,
        description="Optional debug detail; only populated when LEAK_EXCEPTION_DETAILS=true",
    )


class ErrorResponse(BaseModel):
    """Standard error response envelope."""

    error: ErrorDetail


AUTH_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {"description": "Authentication required", "model": ErrorResponse},
    403: {"description": "Insufficient privileges", "model": ErrorResponse},
}

NOT_FOUND_RESPONSES: dict[int | str, dict[str, Any]] = {
    404: {"description": "Resource not found", "model": ErrorResponse},
}

AUTH_AND_NOT_FOUND: dict[int | str, dict[str, Any]] = {
    **AUTH_RESPONSES,
    **NOT_FOUND_RESPONSES,
}
