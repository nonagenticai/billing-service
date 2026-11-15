ARG BASE_REGISTRY=docker.io

FROM ${BASE_REGISTRY}/library/python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.10.10 /uv /uvx /usr/local/bin/

WORKDIR /app

COPY pyproject.toml uv.lock* ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY src/ src/
COPY alembic/ alembic/
COPY alembic.ini ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


FROM ${BASE_REGISTRY}/library/python:3.14-slim AS runtime

RUN groupadd --gid 1000 app && \
    useradd --uid 1000 --gid app --shell /bin/bash --create-home app

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY src/ src/
COPY alembic/ alembic/
COPY alembic.ini ./
# scripts/ is needed for the billing-pricing-seed CronJob (runs
# scripts/seed_initial_pricing.py against /api/config/provider-pricing).
# Without this COPY the CronJob silently fails with "No such file or
# directory" → BackoffLimitExceeded. Discovered 2026-05-09 — pricing-seed
# had been failing for ≥ 4h before noticed.
COPY scripts/ scripts/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

USER app

EXPOSE 8157

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8157", "--workers", "1"]
