"""Structured logging with OpenTelemetry trace correlation."""

import logging
import os
import sys
import types
from typing import TYPE_CHECKING

import structlog
from structlog.typing import EventDict, Processor, WrappedLogger

if TYPE_CHECKING:
    from settings import Settings

OTEL_AVAILABLE = False
if TYPE_CHECKING:
    trace: types.ModuleType | None = None
else:
    try:
        from opentelemetry import trace

        OTEL_AVAILABLE = True
    except ImportError:
        trace = None


def add_trace_context(
    logger: WrappedLogger,
    _method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """Inject OpenTelemetry trace context into structured logs."""
    if OTEL_AVAILABLE and trace:
        span = trace.get_current_span()
        if span and span.is_recording():
            ctx = span.get_span_context()
            if ctx.is_valid:
                event_dict["trace_id"] = format(ctx.trace_id, "032x")
                event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def add_service_context(
    logger: WrappedLogger,
    _method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """Add service metadata to logs."""
    event_dict["service"] = os.getenv("OTEL_SERVICE_NAME", "billing-service")
    event_dict["environment"] = os.getenv("ENVIRONMENT", "production")
    return event_dict


_configured: bool = False


def configure_logging(settings: Settings | None = None, *, level: int | None = None) -> None:
    """Configure structured JSON logging with trace context."""
    global _configured

    if _configured:
        return

    if settings is not None:
        log_format = settings.log_format.lower()
        log_level = level or getattr(logging, settings.log_level.upper())
    else:
        log_format = os.getenv("LOG_FORMAT", "json").lower()
        log_level = level or getattr(
            logging,
            os.getenv("LOG_LEVEL", "INFO").upper(),
        )

    if log_format == "json":
        processors: list[Processor] = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            add_service_context,
            add_trace_context,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            add_service_context,
            add_trace_context,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=log_level)

    for logger_name in ("httpx", "httpcore", "uvicorn.access", "urllib3"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    _configured = True
