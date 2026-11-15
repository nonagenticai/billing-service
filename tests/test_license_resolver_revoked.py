"""A REVOKED licence must stop attracting derived invoice attribution.

THE DEFECT
----------
``services.license_resolver`` names which of a tenant's licences a licence-less
cost event belongs to, and the only table it reads is ``request_quota_config``
— billing's local mirror of the licence registry. Until migration 0025 that
table carried ``license_id, tenant_id, quota_monthly, app_id, app_name,
created_at, updated_at`` and **no lifecycle column at all**.

The resolver's own module docstring recorded the consequence as a LIMITATION: a
REVOKED licence stays resolvable and its tenant keeps attracting attributed
invoice lines, because the only predicate the two map-builders could apply was
``app_id IS NOT NULL``. Measured on the licences registry 2026-09-08: **43
unrevoked, unexpired tokens hang off ``status='revoked'`` licences** (control:
177 hang off active ones) — 35 on ``n8n-dev``/Licenses Module, plus ahtapol
FR/KAG and four singletons. Registry-wide, 92 of 258 licences are revoked.

WHY THESE TESTS ARE DATABASE-BACKED AND NOT ``_FakeSession``-BACKED
-------------------------------------------------------------------
``tests/test_license_resolver.py`` drives ``_build_map`` through a tiny
stand-in session that returns rows directly. That harness is right for the
ambiguity RULES, but it is structurally incapable of testing this fix: the
stand-in never evaluates a WHERE clause, so a test written against it passes
identically with and without the ``status`` predicate — it would prove nothing.

Every test below therefore runs against a real database, so the predicate is
executed by Postgres, and the revoked row is filtered (or not) for real.

WHAT FAILS ON ``origin/main``
-----------------------------
All three. On main ``request_quota_config`` has no ``status`` column, so
``mark_quota_config_status`` does not exist and the column cannot be written or
selected. That is not an incidental import error — it IS the defect: main has no
way to express "this licence is revoked", which is exactly why the resolver kept
attributing to one.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from models.db import ACTIVE_LICENSE_STATUS, RequestQuotaConfig
from services.license_resolver import _build_app_map, _build_map
from services.quota import mark_quota_config_status, upsert_request_quota_config
from tests.conftest import T

_APP_NAME = "Licenses Module"


async def _seed(
    session,
    license_id: uuid.UUID,
    *,
    tenant: str,
    app_id: uuid.UUID,
    app_name: str = _APP_NAME,
) -> None:
    """Mirror one licence exactly as ``sync-from-licenses`` would."""
    await upsert_request_quota_config(
        session,
        license_id,
        T(tenant),
        1000,
        app_id=app_id,
        app_name=app_name,
    )


class TestRevokedLicencesAreNotAttributionCandidates:
    @pytest.mark.unit
    async def test_a_revoked_licence_is_dropped_from_the_tenant_app_map(self, db_session):
        """The (tenant, app) -> licence map must not offer a revoked licence.

        This is the invoice-facing half: ``_build_map`` is what turns a
        licence-less cost event into a licence-keyed invoice line. A revoked
        licence surviving here is a real customer being billed a derived line
        against a licence they no longer hold.

        ⛔ The seeded tenant owns EXACTLY ONE licence for this app. That matters:
        if the revoked row merely made the pair AMBIGUOUS, `_build_map` would
        drop the key for a completely different reason (rule 2) and the test
        would pass without the status predicate existing. One licence, one app,
        one tenant — so the only thing that can remove it is the filter.
        """
        tenant = "revoked-map"
        revoked = uuid.uuid4()
        await _seed(db_session, revoked, tenant=tenant, app_id=uuid.uuid4())

        # POSITIVE CONTROL — while it is active, it resolves. Without this the
        # test could pass because the seed never landed at all.
        built = await _build_map(db_session)
        assert built.get((T(tenant), _APP_NAME.casefold())) == revoked

        transitioned = await mark_quota_config_status(db_session, revoked, "revoked")
        assert transitioned is True, "the mirror row should have moved out of 'active'"

        built_after = await _build_map(db_session)
        assert (T(tenant), _APP_NAME.casefold()) not in built_after

    @pytest.mark.unit
    async def test_a_revoked_licence_is_dropped_from_the_licence_app_map(self, db_session):
        """``_build_app_map`` must agree with ``_build_map`` on "resolvable".

        Both maps are consulted on the same flush path. A licence excluded from
        one but present in the other would stamp an ``app_id`` onto cost the
        (tenant, app) map refuses to attribute — a half-attributed row, which is
        worse than an unattributed one because it looks complete.
        """
        tenant = "revoked-appmap"
        revoked, app_id = uuid.uuid4(), uuid.uuid4()
        await _seed(db_session, revoked, tenant=tenant, app_id=app_id)

        assert (await _build_app_map(db_session)).get(revoked) == app_id  # positive control

        await mark_quota_config_status(db_session, revoked, "revoked")

        assert revoked not in await _build_app_map(db_session)

    @pytest.mark.unit
    async def test_an_active_licence_beside_a_revoked_one_still_resolves(self, db_session):
        """The filter must remove the revoked licence and NOTHING else.

        The failure mode this guards is the opposite of the defect: a predicate
        that over-matches would silently stop attributing live spend, which is
        the same lost-invoice-line outcome arrived at from the other side. The
        two licences here belong to DIFFERENT tenants so neither can mask the
        other through the ambiguity rule.
        """
        live, revoked = uuid.uuid4(), uuid.uuid4()
        await _seed(db_session, live, tenant="revoked-live", app_id=uuid.uuid4())
        await _seed(db_session, revoked, tenant="revoked-dead", app_id=uuid.uuid4())

        await mark_quota_config_status(db_session, revoked, "revoked")

        built = await _build_map(db_session)
        assert built.get((T("revoked-live"), _APP_NAME.casefold())) == live
        assert (T("revoked-dead"), _APP_NAME.casefold()) not in built

    @pytest.mark.unit
    async def test_an_unrecognised_status_fails_closed(self, db_session):
        """A status this service does not model must NOT resolve.

        The predicate is ``== 'active'`` and deliberately not ``!= 'revoked'``.
        If the registry grows a new lifecycle value ('suspended', say), the
        licence must drop out of attribution until someone widens
        ``LICENSE_STATUSES`` — never keep attributing because the new value did
        not happen to be spelled 'revoked'.

        ``pending`` is used as the probe because it is a real member of the
        registry's own CHECK constraint that this service treats as
        non-resolvable, so the row is writable and the assertion is about the
        PREDICATE rather than about a constraint violation.
        """
        tenant = "revoked-pending"
        lic = uuid.uuid4()
        await _seed(db_session, lic, tenant=tenant, app_id=uuid.uuid4())

        await mark_quota_config_status(db_session, lic, "pending")

        assert (T(tenant), _APP_NAME.casefold()) not in await _build_map(db_session)
        assert lic not in await _build_app_map(db_session)


class TestMarkQuotaConfigStatus:
    @pytest.mark.unit
    async def test_marking_keeps_the_row_and_its_quota(self, db_session):
        """Revocation MARKS the mirror row; it does not delete it.

        ``delete_absent_quota_configs`` used to be the only thing that removed a
        revoked licence's row, and deleting is lossy: the row carries the
        enforcement limit and ``quota_check`` falls back to a CALLER-SUPPLIED
        ``quota_monthly`` when no row exists, so the hard delete traded a stale
        attribution for a fail-open quota. Keeping the row and marking it gets
        both halves.
        """
        lic = uuid.uuid4()
        await _seed(db_session, lic, tenant="revoked-keep", app_id=uuid.uuid4())

        await mark_quota_config_status(db_session, lic, "revoked")

        row = (
            await db_session.execute(
                select(RequestQuotaConfig).where(RequestQuotaConfig.license_id == lic)
            )
        ).scalar_one()
        assert row.status == "revoked"
        assert row.quota_monthly == 1000, "the enforcement limit must survive revocation"

    @pytest.mark.unit
    async def test_marking_is_a_no_op_for_a_row_that_already_holds_the_status(self, db_session):
        """Returns False when nothing moved, so the caller counts TRANSITIONS.

        Without the ``status != status`` guard every already-revoked row is
        rewritten on every reconcile: ``updated_at`` churns and the endpoint's
        ``quota_configs_marked_non_active`` would report "rows examined" while
        being read as "rows newly revoked".
        """
        lic = uuid.uuid4()
        await _seed(db_session, lic, tenant="revoked-noop", app_id=uuid.uuid4())

        assert await mark_quota_config_status(db_session, lic, "revoked") is True
        assert await mark_quota_config_status(db_session, lic, "revoked") is False

    @pytest.mark.unit
    async def test_marking_an_unmirrored_licence_creates_nothing(self, db_session):
        """UPDATE-if-present: a licence with no mirror row must not gain one.

        The sync now sees every revoked licence in the registry (92 of 258
        measured 2026-09-08), and almost none of them have ever had a mirror
        row. An upsert here would manufacture a ``request_quota_config`` — and
        therefore a configured quota limit — for each of them.
        """
        never_mirrored = uuid.uuid4()

        assert await mark_quota_config_status(db_session, never_mirrored, "revoked") is False

        rows = (
            (
                await db_session.execute(
                    select(RequestQuotaConfig.license_id).where(
                        RequestQuotaConfig.license_id == never_mirrored
                    )
                )
            )
            .scalars()
            .all()
        )
        assert rows == []


class TestProvisioningCannotReactivateARevokedLicence:
    @pytest.mark.unit
    async def test_a_status_less_upsert_does_not_clobber_a_revoked_row(self, db_session):
        """``POST /api/quota/config`` must not silently un-revoke a licence.

        That route provisions a quota without knowing anything about lifecycle,
        so it calls ``upsert_request_quota_config`` with ``status=None``. If the
        upsert wrote the column's default on conflict it would flip a revoked
        row back to 'active' and re-open the defect through the other writer —
        the same never-clobber-with-None contract ``app_id``/``app_name``
        already have.
        """
        lic, app_id = uuid.uuid4(), uuid.uuid4()
        await _seed(db_session, lic, tenant="revoked-reupsert", app_id=app_id)
        await mark_quota_config_status(db_session, lic, "revoked")

        # A later provisioning call that knows nothing about lifecycle.
        await upsert_request_quota_config(
            db_session, lic, T("revoked-reupsert"), 5000, app_id=app_id, app_name=_APP_NAME
        )

        row = (
            await db_session.execute(
                select(RequestQuotaConfig).where(RequestQuotaConfig.license_id == lic)
            )
        ).scalar_one()
        assert row.status == "revoked", "a status-less upsert must not re-activate the licence"
        assert row.quota_monthly == 5000, "the quota itself must still be updated"

    @pytest.mark.unit
    async def test_a_new_row_defaults_to_active(self, db_session):
        """A licence mirrored with no status must stay resolvable.

        This is the behaviour-preservation half of migration 0025's backfill: a
        brand-new row written by a caller that does not speak lifecycle has to
        keep attributing exactly as it did before the column existed, or the
        deploy silently stops invoicing live licences.
        """
        lic = uuid.uuid4()
        await _seed(db_session, lic, tenant="revoked-default", app_id=uuid.uuid4())

        row = (
            await db_session.execute(
                select(RequestQuotaConfig).where(RequestQuotaConfig.license_id == lic)
            )
        ).scalar_one()
        assert row.status == ACTIVE_LICENSE_STATUS
        assert (T("revoked-default"), _APP_NAME.casefold()) in await _build_map(db_session)
