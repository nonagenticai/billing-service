"""
Request Logging Middleware.

Provides comprehensive request/response logging with timing information and request tracking.

This middleware:
    - Pulls the canonical request_id from RequestIdMiddleware (which runs first)
    - Falls back to a fresh uuid7 only if that middleware was not wired in
    - Logs incoming requests with method and path
    - Measures request processing time
    - Adds custom headers to responses (X-Request-ID, X-Processing-Time)
    - Logs response status and timing information
"""

import logging
import time
import uuid
from collections.abc import Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

logger: logging.Logger = logging.getLogger(__name__)


_SKIP_LOG_PATHS = frozenset({"/health", "/healthz", "/ready", "/readyz", "/metrics"})


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for logging requests and adding request tracking headers.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip logging for health/probe endpoints to avoid log spam
        if request.url.path in _SKIP_LOG_PATHS:
            return await call_next(request)

        # Pull request_id from RequestIdMiddleware (which runs first); fall
        # back to a fresh uuid7 only if that middleware was not wired in.
        request_id: str = getattr(request.state, "request_id", "") or str(uuid.uuid7())
        request.state.request_id = request_id

        # Start timer
        start_time: float = time.time()

        # Log request
        logger.info(f"Request started: {request.method} {request.url.path} [{request_id}]")

        # Process request
        response: Response = await call_next(request)

        # Calculate processing time
        process_time: float = (time.time() - start_time) * 1000  # Convert to ms

        # Add custom headers
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Processing-Time"] = f"{process_time:.2f}ms"

        # Log response
        logger.info(
            f"Request completed: {request.method} {request.url.path} "
            f"[{request_id}] - Status: {response.status_code} - "
            f"Time: {process_time:.2f}ms"
        )

        return response
