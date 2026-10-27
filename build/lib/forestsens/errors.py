"""Mirrors forestsens/api/errors.py's ApiError shape on the server side --
`code` is the same machine-readable string a caller would see there
("unauthenticated", "not_found", "validation_error", "conflict", ...),
not a generic HTTP-status message. Every real ForestSens API response
carries an envelope of the shape {"data", "meta", "error"}; this
exception is raised whenever that envelope's "error" is non-null (or
the HTTP status itself is an error and there's no parseable envelope
at all, e.g. the Gateway's own generic 401 body).
"""

from __future__ import annotations

from typing import Any


class ForestSensAPIError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None) -> None:
        super().__init__(f"{code}: {message} (HTTP {status})")
        self.status = status
        self.code = code
        self.message = message
        self.details = details if details is not None else []


class BatchFailedError(ForestSensAPIError):
    """Raised by Client.wait_for_batch when the batch itself reaches
    status="failed" -- distinct from a transport/API error, since the
    request that created/polled the batch succeeded fine. Carries the
    last-seen batch dict so a caller can inspect step-level detail
    (steps[].error / steps[].log_tail) without a second call.
    """

    def __init__(self, batch: dict[str, Any]) -> None:
        self.batch = batch
        message = batch.get("error_message") or "batch failed"
        super().__init__(status=200, code="batch_failed", message=message, details=batch.get("steps"))
