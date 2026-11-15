"""``request_quota_config`` must not keep rows for licences that are gone.

THE DEFECT
----------
``request_quota_config`` is a local MIRROR of the licence registry, written only
by the operator-triggered ``POST /api/budget/sync-from-licenses``. There is no
timer, and nothing ever removed a row when a licence was revoked, expired, or
deleted upstream — so the mirror only grew staler.

Measured on production 2026-09-02: **183 rows, 26 of them junk** — 18 whose
``license_id`` did not exist in the registry at all, and 8 whose licence was
``expired`` or ``revoked``.

That is not cosmetic. The mirror is what the attribution resolver reads, and
``_build_map`` keys on ``(tenant_id, app_name.casefold())`` and DROPS any key
with more than one candidate rather than picking one. So a stale row can collide
with a live licence and silently make the whole tenant+app pair unattributable —
``license_id`` NULL, then ``app_id`` NULL, then spend that reaches no invoice
line.

WHY DELETE-BY-ABSENCE AND NOT TRUNCATE-AND-RELOAD
-------------------------------------------------
The sync fetches ``?status=active`` only. A full replace driven by a partial
fetch would drop rows for licences that are merely not in this page, and a
missing row makes ``quota_check`` fall back to a CALLER-SUPPLIED
``quota_monthly`` — i.e. it fails OPEN. Hence the empty-set refusal below, which
is the one place this helper deliberately differs from its budget-side sibling.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from models.db import RequestQuotaConfig
from services.budget import EmptyActiveSetError, delete_absent_quota_configs
from services.quota import upsert_request_quota_config
from tests.conftest import T


async def _seed(session, license_id: uuid.UUID, *, tenant: str = "t1", quota: int = 1000) -> None:
    await upsert_request_quota_config(
        session,
        license_id,
        T(tenant),
        quota,
        app_id=uuid.uuid4(),
        app_name="KAG Module",
    )


async def _mirror_ids(session) -> set[uuid.UUID]:
    rows = (await session.execute(select(RequestQuotaConfig.license_id))).scalars().all()
    return set(rows)


class TestAbsentRowsAreRemoved:
    @pytest.mark.unit
    async def test_a_licence_absent_upstream_loses_its_mirror_row(self, db_session):
        keep, gone = uuid.uuid4(), uuid.uuid4()
        await _seed(db_session, keep)
        await _seed(db_session, gone)

        deleted = await delete_absent_quota_configs(db_session, active_license_ids={keep})

        assert deleted == [gone]
        assert await _mirror_ids(db_session) == {keep}

    @pytest.mark.unit
    async def test_it_is_a_no_op_when_the_mirror_already_matches(self, db_session):
        keep = uuid.uuid4()
        await _seed(db_session, keep)

        deleted = await delete_absent_quota_configs(db_session, active_license_ids={keep})

        assert deleted == []
        assert await _mirror_ids(db_session) == {keep}

    @pytest.mark.unit
    async def test_an_extra_active_licence_not_in_the_mirror_deletes_nothing(self, db_session):
        """The active set is a superset in normal operation — that must be safe."""
        keep = uuid.uuid4()
        await _seed(db_session, keep)

        deleted = await delete_absent_quota_configs(
            db_session, active_license_ids={keep, uuid.uuid4(), uuid.uuid4()}
        )

        assert deleted == []
        assert await _mirror_ids(db_session) == {keep}

    @pytest.mark.unit
    async def test_the_stale_row_that_manufactures_ambiguity_is_the_one_removed(self, db_session):
        """The production shape: two rows on one (tenant, app), one of them dead.

        This is the case that silently zeroes a tenant's attribution.
        """
        live, revoked_upstream = uuid.uuid4(), uuid.uuid4()
        await _seed(db_session, live, tenant="acme")
        await _seed(db_session, revoked_upstream, tenant="acme")
        assert len(await _mirror_ids(db_session)) == 2

        deleted = await delete_absent_quota_configs(db_session, active_license_ids={live})

        assert deleted == [revoked_upstream]
        assert await _mirror_ids(db_session) == {live}


class TestTheEmptySetIsRefused:
    @pytest.mark.unit
    async def test_an_empty_active_set_raises_instead_of_wiping_the_mirror(self, db_session):
        """ "Zero active licences" and "the fetch came back empty" are the same
        signal here, and only one of them is survivable — so refuse both."""
        keep = uuid.uuid4()
        await _seed(db_session, keep)

        with pytest.raises(EmptyActiveSetError):
            await delete_absent_quota_configs(db_session, active_license_ids=set())

        assert await _mirror_ids(db_session) == {keep}, "the mirror must be untouched"
