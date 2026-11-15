"""The attribution mirror must be ABLE to say "this licence is not active".

This module is deliberately written in ``origin/main``'s own vocabulary. It
imports nothing that migration 0025 introduced — no ``ACTIVE_LICENSE_STATUS``,
no ``mark_quota_config_status`` — and reaches the ``status`` column through raw
SQL only.

WHY THAT MATTERS. Its sibling ``tests/test_license_resolver_revoked.py`` cannot
even be COLLECTED on main: it imports symbols that do not exist there, so its
failure is an ``ImportError`` and reads as "the feature is missing" rather than
"the behaviour is wrong". This module fails on main one layer deeper and far
more usefully — Postgres itself raises ``UndefinedColumn: column "status" of
relation "request_quota_config" does not exist``, which is the defect stated as
a fact about the schema: **main has no way to represent a revoked licence, so
the resolver has no way to exclude one.**

That is the whole bug. ``services.license_resolver``'s module docstring recorded
it as a standing LIMITATION, and the live consequence measured on the licences
registry 2026-09-08 was 43 unrevoked, unexpired tokens hanging off
``status='revoked'`` licences — spend still attracting derived invoice
attribution against licences their holders no longer have.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from services.license_resolver import _build_map
from services.quota import upsert_request_quota_config
from tests.conftest import T

_APP_NAME = "KAG Module"


@pytest.mark.unit
async def test_the_mirror_can_express_a_revoked_licence_and_the_resolver_honours_it(db_session):
    """Seed a mirror row, revoke it in SQL, and it must leave the map.

    The three steps are the defect end to end:

    1. A licence is mirrored and resolves — the positive control, without which
       a passing test could just mean the seed never landed.
    2. ``UPDATE ... SET status = 'revoked'`` — the step main cannot execute.
    3. The (tenant, app) key is gone, so no derived invoice line can be stamped
       with this licence any more.

    One tenant, one app, one licence: the key cannot be dropped by the
    ambiguity rule (``_build_map`` rule 2), so the only thing that can remove it
    is the status predicate.
    """
    tenant, lic = "status-col", uuid.uuid4()
    await upsert_request_quota_config(
        db_session, lic, T(tenant), 1000, app_id=uuid.uuid4(), app_name=_APP_NAME
    )

    key = (T(tenant), _APP_NAME.casefold())
    assert (await _build_map(db_session)).get(key) == lic, "positive control: it should resolve"

    await db_session.execute(
        text("UPDATE request_quota_config SET status = 'revoked' WHERE license_id = :lic"),
        {"lic": str(lic)},
    )
    await db_session.flush()

    assert key not in await _build_map(db_session)


@pytest.mark.unit
async def test_the_status_column_rejects_a_value_the_registry_would_reject(db_session):
    """``ck_request_quota_status`` mirrors licences' own ``ck_license_status``.

    The mirror must not be able to hold a lifecycle value the registry itself
    would refuse. Without the constraint, a typo ('actve') would be stored
    happily and the licence would silently stop resolving — a lost invoice line
    with no error anywhere.
    """
    lic = uuid.uuid4()
    await upsert_request_quota_config(
        db_session, lic, T("status-ck"), 1000, app_id=uuid.uuid4(), app_name=_APP_NAME
    )

    with pytest.raises(Exception, match="ck_request_quota_status"):
        await db_session.execute(
            text("UPDATE request_quota_config SET status = 'actve' WHERE license_id = :lic"),
            {"lic": str(lic)},
        )
        await db_session.flush()
