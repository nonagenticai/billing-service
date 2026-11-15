"""Licence/app resolution on the USAGE path (`services.event_flusher`).

WHY THIS FILE EXISTS. `services.license_resolver` has existed and been reviewed
for months, and was called from `services.cost_flusher` ONLY. The usage path took
`license_id` and `app_id` straight off the wire and stored whatever arrived —
there was no lookup at all. Measured on prod over 7 days: `billing_metering_events`
held 70,658 rows, 100% tenant-attributed, with `license_id` AND `app_id` non-null
on 444 of them: **0.628%**. Lifetime, 499 of 78,353.

It cannot be fixed upstream. fr's emitter hardcodes `license_id: ''` / `app_id: ''`
because fr-backend does not sit behind license-authz and does not hold either id
at its emit point. The tenant IS known, and tenant + service is exactly what the
resolver keys on.

⛔ EVERY TEST HERE READS THE ROW BACK OUT OF POSTGRES rather than inspecting the
dict the flusher mutated. The change is only real if it lands in the column; a
mutated in-memory dict proves nothing about the INSERT. That is the same standard
`tests/test_cost_flusher.py::TestAppDerivation` holds itself to.

⚠️ THE FAILURE DIRECTION IS THE POINT. An unattributed metering row is bad; a LOST
one is billing volume. So the error-path tests assert that the event is still
STORED, not merely that a counter moved.
"""

import uuid

import pytest
from sqlalchemy import func, select

from models.db import NULL_PLAN_ID, MeteringEvent, MeteringMonthlyUsage, RequestQuotaConfig
from services.aggregator import flush_events
from tests.conftest import T

pytestmark = pytest.mark.unit

_APP_NAME = "Workers Module"
#: The operator setting the licence half is gated on, in its real CSV shape.
_APP_MAP = f"workers={_APP_NAME}"


@pytest.fixture(autouse=True)
def _fresh_resolver_caches():
    """Both resolver maps are process-global with a 300s TTL.

    Reset on the way IN and OUT, for the two reasons
    `tests/test_cost_flusher.py::TestAppDerivation` documents: a map built by an
    earlier test (against an already-truncated table, i.e. EMPTY) would be reused
    here and every assertion would fail for a reason absent from this file — and a
    map built HERE must not leak into a later test and resolve a licence it does
    not expect. `get_settings` is cleared alongside, because the env override
    below goes through an lru_cache that would otherwise outlive
    `monkeypatch.undo()`.
    """
    from services.license_resolver import reset_cache
    from settings import get_settings

    reset_cache()
    get_settings.cache_clear()
    yield
    reset_cache()
    get_settings.cache_clear()


def _patch_app_map(monkeypatch, value: str = _APP_MAP) -> None:
    """Override `cost_attribution_app_by_service_key` for one test."""
    from settings import get_settings

    monkeypatch.setenv("COST_ATTRIBUTION_APP_BY_SERVICE_KEY", value)
    get_settings.cache_clear()


async def _seed_licence(
    db_session_factory,
    *,
    tenant_id: str,
    license_id: uuid.UUID,
    app_id: uuid.UUID | None,
    app_name: str | None = _APP_NAME,
) -> None:
    """Mirror one licence row exactly as the licences reconcile writes it."""
    async with db_session_factory() as session:
        session.add(
            RequestQuotaConfig(
                license_id=license_id,
                tenant_id=tenant_id,
                quota_monthly=1_000_000,
                app_id=app_id,
                app_name=app_name,
            )
        )
        await session.commit()


def _event(*, tenant: str, license_id: str = "", app_id: str = "", plan_id: str = "") -> dict:
    """A usage event with the ids set EXACTLY as given.

    Deliberately not `tests._aggregator_helpers._make_event`: that builder
    substitutes a fresh uuid for a blank id, which makes the premise of every test
    in this file unreachable. A test that cannot construct its own premise passes
    for the wrong reason.
    """
    return {
        "tenant_id": T(tenant),
        "license_id": license_id,
        "app_id": app_id,
        "plan_id": plan_id,
        "method": "GET",
        "path": "/workers/api/tasks",
        "status": "allowed",
        "source_ip": "10.0.0.1",
        "_service_key": "workers",  # pragma: allowlist secret
    }


async def _flush_one(db_session_factory, tenant: str, **kwargs) -> int:
    """Flush exactly one event for ``tenant`` and return the flushed row count."""
    return await flush_events([_event(tenant=tenant, **kwargs)], db_session_factory, None)


async def _stored(db_session_factory, tenant: str) -> MeteringEvent:
    stmt = select(MeteringEvent).where(MeteringEvent.tenant_id == T(tenant))
    async with db_session_factory() as session:
        rows = (await session.execute(stmt)).scalars().all()
    assert len(rows) == 1, f"expected exactly one stored event, got {len(rows)}"
    return rows[0]


async def _monthly_rows(db_session_factory) -> int:
    async with db_session_factory() as session:
        return (
            await session.execute(select(func.count()).select_from(MeteringMonthlyUsage))
        ).scalar()


class TestLicenceResolutionOnTheUsagePath:
    async def test_a_licence_less_event_is_stored_with_its_tenants_licence(
        self, db_session_factory, monkeypatch
    ):
        """THE HEADLINE, and it FAILS on unmodified code.

        Before this change `event_flusher` called no resolver at all, so
        `license_id` and `app_id` stayed NULL and both assertions below fail. This
        is the 0.628% defect reduced to one event.
        """
        tenant = T("usage-resolve-headline")
        lic, app = uuid.uuid4(), uuid.uuid4()
        await _seed_licence(db_session_factory, tenant_id=tenant, license_id=lic, app_id=app)
        _patch_app_map(monkeypatch)

        assert await _flush_one(db_session_factory, "usage-resolve-headline") == 1

        row = await _stored(db_session_factory, "usage-resolve-headline")
        assert row.license_id == lic, "the tenant's own licence for this service was not resolved"
        assert row.app_id == app, "the app derivable from that licence was not stamped"

    async def test_an_explicit_wire_licence_is_never_overwritten(
        self, db_session_factory, monkeypatch
    ):
        """⛔ THE EMITTER WINS. A resolved value must never replace an asserted one.

        The mirror is seeded so resolution WOULD succeed and would return a
        DIFFERENT licence — so this passes only because the loop skips rows that
        already carry an id, not because resolution happened to be unavailable.
        That distinction is the whole test; a version of it with an empty mirror
        would pass on broken code.
        """
        tenant = T("usage-resolve-explicit")
        mirrored, asserted = uuid.uuid4(), uuid.uuid4()
        await _seed_licence(
            db_session_factory, tenant_id=tenant, license_id=mirrored, app_id=uuid.uuid4()
        )
        _patch_app_map(monkeypatch)

        event = _event(tenant="usage-resolve-explicit", license_id=str(asserted))
        assert await flush_events([event], db_session_factory, None) == 1

        row = await _stored(db_session_factory, "usage-resolve-explicit")
        assert row.license_id == asserted
        assert row.license_id != mirrored, (
            "billing inferred a licence over one the emitter asserted; a derived "
            "value that is indistinguishable from an asserted one is how a guess "
            "later gets quoted as evidence"
        )

    async def test_a_resolver_error_does_not_drop_the_event(self, db_session_factory, monkeypatch):
        """⛔ THE FAILURE DIRECTION. A broken resolver costs attribution, never the row.

        An unattributed metering row is bad. A LOST one is billing volume, and
        billing volume is not recoverable from a counter. So a raising map build
        must leave today's exact behaviour — the id stays NULL — and the event must
        still reach postgres.
        """
        tenant = T("usage-resolve-error")
        await _seed_licence(
            db_session_factory, tenant_id=tenant, license_id=uuid.uuid4(), app_id=uuid.uuid4()
        )
        _patch_app_map(monkeypatch)

        def _boom(*_args, **_kwargs):
            raise RuntimeError("resolution map unavailable")

        monkeypatch.setattr("services.event_flusher.get_resolution_map", _boom)
        monkeypatch.setattr("services.event_flusher.get_app_map", _boom)

        assert await _flush_one(db_session_factory, "usage-resolve-error") == 1

        row = await _stored(db_session_factory, "usage-resolve-error")
        assert row.license_id is None
        assert row.app_id is None

    async def test_an_unmappable_service_key_stays_null_rather_than_guessing(
        self, db_session_factory, monkeypatch
    ):
        """Rule 1 of the resolver: never fabricate.

        The licence exists in the mirror, but the operator map names no app for
        this service_key, so there is no (tenant, app) key to look up. NULL is the
        correct answer — a wrong license_id is an invented invoice line and is
        strictly worse than a missing one.
        """
        tenant = T("usage-resolve-unmapped")
        await _seed_licence(
            db_session_factory, tenant_id=tenant, license_id=uuid.uuid4(), app_id=uuid.uuid4()
        )
        _patch_app_map(monkeypatch, "kag=Some Other App")

        assert await _flush_one(db_session_factory, "usage-resolve-unmapped") == 1

        row = await _stored(db_session_factory, "usage-resolve-unmapped")
        assert row.license_id is None

    async def test_resolution_unlocks_the_monthly_usage_row_only_with_a_plan(
        self, db_session_factory, monkeypatch
    ):
        """WHAT RESOLUTION GETS for the license-authz-shaped population.

        With a licence resolved AND a plan_id on the wire, the outer guard
        `if license_id:` passes (it always would have, even before commit
        `41a47d0` widened it from `if license_id and plan_id:`) and quota is
        counted, keyed on this event's own real plan_id.

        ⚠️ CORRECTED. This used to say fr's plan-less traffic "still writes no
        usage counter" and pointed at the sibling test below as proof. Migration
        `0035_monthly_usage_plan_default.py` put a NOT-NULL sentinel default
        (`NULL_PLAN_ID`) on `MeteringMonthlyUsage.plan_id` and `41a47d0` widened the
        guard, so a resolved, plan-less licence — fr's real shape — now writes ONE
        row keyed on the sentinel instead of none. The sibling test below now pins
        THAT repaired behaviour, not the old skip.
        """
        tenant = T("usage-resolve-monthly")
        lic = uuid.uuid4()
        await _seed_licence(
            db_session_factory, tenant_id=tenant, license_id=lic, app_id=uuid.uuid4()
        )
        _patch_app_map(monkeypatch)

        event = _event(tenant="usage-resolve-monthly", plan_id=str(uuid.uuid4()))
        assert await flush_events([event], db_session_factory, None) == 1

        assert await _monthly_rows(db_session_factory) == 1

    async def test_a_resolved_licence_without_a_plan_now_writes_one_row_under_the_sentinel(
        self, db_session_factory, monkeypatch
    ):
        """THE CONTROL for the test above — and fr's real shape.

        ⚠️ CORRECTED. This used to assert "no monthly row without a plan" — that
        wording described the DEFECT (quota could never bite for fr-shaped
        traffic, since billing cannot obtain a plan_id: `request_quota_config` has
        no plan column, there is no Plan model, no `resolve_plan`). Migration
        `alembic/versions/0035_monthly_usage_plan_default.py` put a NOT-NULL
        sentinel DEFAULT (`NULL_PLAN_ID`) on `MeteringMonthlyUsage.plan_id` — the
        same sentinel `hourly`/`daily` already coalesce to — and commit `41a47d0`
        widened `event_flusher.py`'s outer guard from
        `if license_id and plan_id:` to `if license_id:`. A resolved, plan-less
        licence now correctly writes exactly ONE monthly row keyed on the
        sentinel. This test now pins that repaired behaviour, not the old skip.
        See `tests/test_monthly_usage_plan_sentinel.py` for the full writeup.
        """
        tenant = T("usage-resolve-noplan")
        lic = uuid.uuid4()
        await _seed_licence(
            db_session_factory, tenant_id=tenant, license_id=lic, app_id=uuid.uuid4()
        )
        _patch_app_map(monkeypatch)

        assert await _flush_one(db_session_factory, "usage-resolve-noplan") == 1

        row = await _stored(db_session_factory, "usage-resolve-noplan")
        assert row.license_id == lic, "premise: the licence must actually have resolved"

        assert await _monthly_rows(db_session_factory) == 1, (
            "a resolved licence with no plan must now be counted under the sentinel, not skipped"
        )
        async with db_session_factory() as session:
            usage = (
                await session.execute(
                    select(MeteringMonthlyUsage).where(MeteringMonthlyUsage.license_id == lic)
                )
            ).scalar_one()
        assert usage.plan_id == NULL_PLAN_ID, (
            "the monthly row must be keyed on the explicit 'no plan asserted' "
            "sentinel, never left to a real plan id the wire never sent"
        )


class TestHostileStatusLabelIsClamped:
    """`status` reaches a Prometheus label straight off the wire.

    A licensed emitter putting a per-request string there minted one series per
    request, per replica, for the life of the process — the cardinality bomb the
    25-key clamp in `services.consumer` exists to prevent, arriving through the
    front door of a different counter.
    """

    @staticmethod
    def _label_values() -> set[str]:
        """Every `status` label value currently exported by the counter.

        ⛔ Read via `.collect()`, NEVER via `.labels(status=raw)._value.get()` —
        `labels()` CREATES the child, so probing for the hostile value that way
        would mint the very series the clamp exists to prevent and the test would
        pass while proving the opposite.
        """
        import observability.metrics as om

        return {
            s.labels["status"]
            for metric in om.metering_status_unrecognised_total.collect()
            for s in metric.samples
            if "status" in s.labels
        }

    @staticmethod
    def _other() -> float:
        import observability.metrics as om

        return om.metering_status_unrecognised_total.labels(status="_other")._value.get()

    async def test_a_hostile_status_lands_in_other_and_mints_no_series(self, db_session_factory):
        """The raw value is counted, but under `_other`, and wins no series of its own."""
        hostile = "ValueError: connection reset by peer at 2026-09-13T10:11:12.345Z req=abc123"
        before = self._other()

        event = _event(
            tenant="status-hostile",
            license_id=str(uuid.uuid4()),
            plan_id=str(uuid.uuid4()),
        )
        event["status"] = hostile

        assert await flush_events([event], db_session_factory, None) == 1

        assert self._other() == before + 1, "the event must still be COUNTED, just not labelled"
        assert hostile not in self._label_values(), (
            "the caller-controlled status value won its own Prometheus series; the "
            "clamp is the only thing standing between one emitter and unbounded "
            "cardinality on every replica"
        )

    async def test_a_known_http_code_keeps_its_own_series(self, db_session_factory):
        """THE CONTROL. Without it a clamp that sent EVERYTHING to `_other` would pass.

        The allowlist exists to keep the diagnosable values diagnosable: `404` must
        still name itself, or the counter stops answering "which code".
        """
        event = _event(
            tenant="status-known",
            license_id=str(uuid.uuid4()),
            plan_id=str(uuid.uuid4()),
        )
        event["status"] = "404"

        assert await flush_events([event], db_session_factory, None) == 1
        assert "404" in self._label_values()


class TestTheNewCountersExist:
    """⛔ THE FORCING FUNCTION, and it is RED until `observability/metrics.py` lands.

    The three increment sites added in this round deliberately swallow a missing
    metric (`services.event_flusher._note_license_resolution` and its sibling use
    the blind-except idiom `services.consumer._note_unmapped_service_key` uses)
    because an observability defect must never be able to DROP a metering event.
    The cost of that choice is that an undefined counter reads as a flat zero,
    which is indistinguishable from "nothing was resolved" — a guard blinded
    rather than failing.

    This test is what makes the absence loud in CI instead of silent in
    production. It passes the moment the definitions listed in the PR body are
    added, and it must never be relaxed into a skip.
    """

    @pytest.mark.parametrize(
        "attr",
        ["metering_license_resolved_total", "metering_app_resolved_total"],
    )
    def test_resolution_counter_is_defined_with_a_result_label(self, attr):
        import observability.metrics as om

        counter = getattr(om, attr, None)
        assert counter is not None, f"observability.metrics.{attr} is not defined"
        for result in ("resolved", "unresolved", "error"):
            counter.labels(result=result)
