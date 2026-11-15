"""Alembic environment for metering_service.

Wires the project's SQLAlchemy declarative Base to Alembic so migrations can
introspect the live model metadata. Reads DATABASE_URL from the environment
(falling back to whatever sqlalchemy.url is in alembic.ini for local-dev /
container shells without env vars).

Per project memory:
- PostgreSQL 18 with psycopg 3.x driver only (no asyncpg).
- pgbouncer requires NullPool — we use NullPool unconditionally here.
"""

from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Make `src/` importable so `from db import Base` and `from models.db import *`
# resolve without an installed package.
SRC_PATH = Path(__file__).resolve().parent.parent / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# Import the model module so Base.metadata is populated with all tables before
# Alembic runs autogenerate.
import models.db  # noqa: E402, F401  -- import for side effect
from db import Base  # noqa: E402  -- import after sys.path mutation

config = context.config

# Honour DATABASE_URL when present; this is what the running services use and
# is the single source of truth in dev + CI + prod.
db_url = os.getenv("DATABASE_URL")
if db_url:
    # Normalise async drivers to the sync psycopg dialect — Alembic uses sync.
    if db_url.startswith("postgresql+asyncpg://"):
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    config.set_main_option("sqlalchemy.url", db_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (no DB connection)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (real DB engine, NullPool for pgbouncer)."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
