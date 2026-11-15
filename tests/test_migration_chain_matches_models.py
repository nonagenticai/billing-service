"""CI runs the chain-vs-models parity check, so it cannot rot again.

``scripts/validate_migrations.py`` existed for months and could not pass: its
scenario replayed historical DDL against a present-day ``create_all`` schema and
died five revisions in. Nothing referenced it outside its own docstring, which
is why nobody noticed. A validator nothing runs is not a validator.

It is invoked as a SUBPROCESS on purpose: the script sets ``DATABASE_URL`` so
alembic picks up its throwaway database, and doing that in-process would leak
the mutation into every test that runs afterwards.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
import sqlalchemy as sa

REPO = Path(__file__).resolve().parent.parent


@pytest.mark.integration
def test_alembic_chain_builds_the_schema_the_models_declare():
    dsn = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    assert dsn, "TEST_DATABASE_URL/DATABASE_URL must be set (conftest requires one too)"
    server = sa.make_url(dsn).set(database="postgres")

    proc = subprocess.run(
        [sys.executable, "scripts/validate_migrations.py"],
        cwd=REPO,
        env={**os.environ, "DATABASE_URL": server.render_as_string(hide_password=False)},
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert proc.returncode == 0, (
        "`alembic upgrade head` does not build the schema the ORM declares. Anything "
        "the models declare but the chain does not build is missing from every "
        "freshly-migrated database -- which is how uq_cost_daily and both "
        "provider_pricing indexes came to exist only in src/db.py's runtime replay "
        f"(fixed by 0015_pricing_cost_daily_shapes).\n\n{proc.stdout}\n{proc.stderr}"
    )
