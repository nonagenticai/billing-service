#!/usr/bin/env python3
"""Export OpenAPI spec from the FastAPI app to docs/openapi.json."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Ensure imports work without a live DB / live env.
# The OpenAPI spec is built from route signatures alone; no DB connection is made.
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://user:pass@localhost:5432/billing",
)
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("LICENSES_URL", "http://localhost:9999")
os.environ.setdefault("SERVICE_NAME", "billing-service")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("LOG_FORMAT", "console")
os.environ.setdefault("OTEL_SDK_DISABLED", "true")
os.environ.setdefault("DASHBOARD_AUTH_ENABLED", "false")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from main import app  # noqa: E402


def main() -> int:
    spec = app.openapi()
    out = ROOT / "docs" / "openapi.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(spec, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {out} ({out.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
