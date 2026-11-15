"""OpenTelemetry setup with graceful degradation."""

import logging
import os
import socket
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from settings import Settings

logger = logging.getLogger(__name__)

_initialized: bool = False


def _test_connection(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True
    except (TimeoutError, ConnectionRefusedError, OSError):  # fmt: skip
        return False


def setup_telemetry(settings: Settings) -> bool:
    """Initialize OpenTelemetry tracer provider with graceful degradation."""
    global _initialized

    if _initialized:
        return True

    if os.getenv("OTEL_SDK_DISABLED", "false").lower() == "true":
        logger.info("OTEL SDK disabled via environment variable")
        return False

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as e:
        logger.warning("OTEL packages not available: %s", e)
        return False

    resource = Resource.create({SERVICE_NAME: settings.service_name})
    provider = TracerProvider(resource=resource)

    try:
        parsed = urlparse(settings.otlp_traces_endpoint.replace("/v1/traces", ""))
        host = parsed.hostname or "alloy"
        port = parsed.port or 4318

        if _test_connection(host, port):
            exporter = OTLPSpanExporter(endpoint=settings.otlp_traces_endpoint, timeout=5)
            processor = BatchSpanProcessor(
                exporter,
                max_queue_size=2048,
                schedule_delay_millis=5000,
                max_export_batch_size=512,
                export_timeout_millis=30000,
            )
            provider.add_span_processor(processor)
            logger.info("OTEL exporter configured: %s", settings.otlp_traces_endpoint)
        else:
            logger.warning("OTLP endpoint not reachable, tracing disabled")
    except (ValueError, OSError) as e:
        logger.warning("Failed to configure OTEL exporter: %s", e)

    trace.set_tracer_provider(provider)

    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()
    except (ImportError, RuntimeError) as e:
        logger.warning("HTTPX instrumentation failed: %s", e)

    _initialized = True
    return True
