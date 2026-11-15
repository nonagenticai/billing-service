"""Quota check service — limits from ``request_quota_config``, usage from ``metering_monthly_usage``."""

import uuid
from collections.abc import Mapping
from datetime import date
from typing import Any, NamedTuple, cast

import structlog
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

import observability.metrics as observability_metrics
from models.db import MeteringMonthlyUsage, RequestQuotaConfig, _utcnow
from timeutil import current_month_start

logger = structlog.stdlib.get_logger(__name__)


async def upsert_request_quota_config(
    session: AsyncSession,
    license_id: uuid.UUID,
    tenant_id: str,
    quota_monthly: int,
    app_id: uuid.UUID | None = None,
    app_name: str | None = None,
    status: str | None = None,
    app_key: str | None = None,
) -> None:
    """Upsert one durable ``request_quota_config`` row (does NOT commit).

    The single source of truth for a license's configured monthly quota.
    Writers: ``POST /api/quota/config`` (provisioning ``register_quota``) and
    ``POST /api/budget/sync-from-licenses`` (plan-seeded).

    ``tenant_id`` is NOT NULL on the row (migration 0007); callers MUST pass a
    non-empty tenant-of-record — a NULL/empty value would raise a NOT NULL
    violation AND re-open the cross-tenant-read gap. The transaction boundary is
    the caller's so this can compose inside the ``sync-from-licenses`` tx
    alongside its audit writes.

    ``app_id``/``app_name`` default to None so the two existing call sites keep
    working unchanged. They are written ONLY when the caller actually has them:
    a reconcile that learns the app must be able to fill it in, but a caller that
    does not know it must never be able to ERASE a previously-synced value --
    that would silently un-resolve every licence it touches. Hence the
    ``COALESCE(EXCLUDED.x, existing.x)`` on update rather than a plain
    assignment.

    ``status`` is the licence's lifecycle state mirrored from the registry
    (migration 0025). It follows the SAME never-clobber-with-None rule as
    ``app_id``/``app_name`` and for the same reason: ``POST /api/quota/config``
    provisions a row without knowing the licence's lifecycle, and if that caller
    could reset the column it would silently RE-ACTIVATE a revoked licence for
    attribution — the exact defect 0025 closes, reintroduced through the other
    writer. A caller that does not know the status passes None and the stored
    value survives; a NEW row with no status supplied takes the column's
    ``'active'`` server default, which matches the behaviour before 0025.

    ``quota_monthly`` IS ASSIGNED UNCONDITIONALLY, and that is the contract. It
    is a REQUIRED positional parameter -- a caller cannot avoid supplying one --
    so supplying one means meaning to set it. Only
    ``status``/``app_id``/``app_name`` carry the never-clobber-with-None rule,
    because those are the ones a caller can legitimately not know.

    ⛔ THE WRITERS DO NOT AGREE ON THE VALUE:

    1. ``POST /api/quota/config`` (``routers.quota.register_quota``) -- the
       PROVISIONED limit, chosen per licence by an operator.
    2. ``POST /api/budget/sync-from-licenses`` (``routers.budget``) -- the PLAN
       limit, seeded from the upstream ``plan.quota_monthly``.

    Writer 2 running after writer 1 silently replaces a provisioned 1,000 with a
    plan 1,000,000, and until now nothing recorded that it had happened because
    :func:`record_quota_config_drift` compared ID SETS ONLY.

    ⛔ AND GATING THIS ASSIGNMENT IS THE WRONG REMEDY -- MEASURED, NOT REASONED.
    An opt-in flag defaulting to "do not move the limit" looks like drift safety,
    but against a required positional it silently converts EVERY existing caller
    into a no-op writer: the provisioning route stops applying the quota it was
    just handed. The full suite refuted it immediately through
    ``test_a_status_less_upsert_does_not_clobber_a_revoked_row``, whose docstring
    states the contract in as many words -- the status must NOT be re-activated
    AND "the quota itself must still be updated". The clobber is real; the remedy
    is to DETECT it. See :func:`record_quota_config_value_drift` and
    ``billing_quota_config_value_drift``, which compare the VALUES that every
    id-set comparison is blind to.


    ⚠️ ``tenant_id`` is STILL assigned unconditionally, and that is a deliberate
    asymmetry rather than an oversight. It is NOT NULL and every caller passes a
    resolved tenant-of-record, so a COALESCE would be a no-op; the real hazard
    (a licence silently re-tenanted by whichever writer ran last) is only
    detectable with a read-before-write, which does not belong behind an upsert
    helper. It is recorded here rather than silently fixed.

    ⚠️ NOT validated here. The CHECK constraint ``ck_request_quota_status`` is
    the enforcement point, so an unrecognised upstream status raises rather than
    being silently coerced to something resolvable.
    """
    stmt = pg_insert(RequestQuotaConfig).values(
        license_id=license_id,
        tenant_id=tenant_id,
        quota_monthly=quota_monthly,
        app_id=app_id,
        app_name=app_name,
        app_key=app_key,
        # Omit entirely when unknown so the INSERT path takes the server
        # default; passing an explicit None would violate NOT NULL.
        **({"status": status} if status is not None else {}),
    )
    upsert = stmt.on_conflict_do_update(
        index_elements=["license_id"],
        set_={
            # Unconditional BY DESIGN, unlike quota_monthly below -- see the
            # docstring's tenant_id paragraph for why a COALESCE here would be a
            # no-op and what the real (unfixed, recorded) hazard is.
            "tenant_id": tenant_id,
            # ⚠️ UNCONDITIONAL, AND THAT IS THE CONTRACT RATHER THAN AN OVERSIGHT.
            # `quota_monthly` is a REQUIRED positional argument: a caller cannot
            # avoid supplying one, so supplying one means meaning to set it.
            # `test_a_status_less_upsert_does_not_clobber_a_revoked_row` pins
            # exactly that -- "the quota itself must still be updated" -- while
            # asserting the status is NOT re-activated, so the two axes are
            # deliberately different and only `status`/`app_id`/`app_name` carry
            # the never-clobber-with-None contract.
            #
            # ⛔ I TRIED TO MAKE THIS CONDITIONAL AND THE SUITE REFUTED IT. Gating
            # it behind an opt-in flag looks like drift safety, but with a
            # required positional it silently turns EVERY existing caller into a
            # no-op writer -- the provisioning route stops applying the quota it
            # was just handed. The hazard it was aimed at is real (three writers
            # upsert on this primary key, so the plan-seeded sync can move a
            # provisioned limit), but the remedy is to DETECT it, not to make the
            # write quietly stop working: see `record_quota_config_value_drift`
            # and `billing_quota_config_value_drift`, which compare the VALUES
            # the id-set gauges are blind to.
            "quota_monthly": quota_monthly,
            # NEVER clobber a known app with None -- see the docstring. EXCLUDED
            # is the row we tried to insert; the bare column is what is already
            # stored, so a caller that does not know the app keeps the old value.
            "app_id": func.coalesce(stmt.excluded.app_id, RequestQuotaConfig.app_id),
            "app_name": func.coalesce(stmt.excluded.app_name, RequestQuotaConfig.app_name),
            # Same never-clobber-with-None contract (migration 0049).
            "app_key": func.coalesce(stmt.excluded.app_key, RequestQuotaConfig.app_key),
            # Same never-clobber-with-None contract as app_id/app_name above --
            # see the docstring. `stmt.excluded.status` is the column's server
            # default ('active') when the caller supplied nothing, so the
            # COALESCE is what stops a status-less provisioning call from
            # re-activating a revoked licence.
            "status": func.coalesce(stmt.excluded.status, RequestQuotaConfig.status)
            if status is not None
            else RequestQuotaConfig.status,
            "updated_at": _utcnow(),
        },
    )
    await session.execute(upsert)


async def mark_quota_config_status(
    session: AsyncSession,
    license_id: uuid.UUID,
    status: str,
) -> bool:
    """Set the lifecycle ``status`` of an EXISTING mirror row (does NOT commit).

    Returns True if a row was actually transitioned, False if there was no row
    or it already held this status. The caller uses that to report how many
    licences a reconcile genuinely made non-resolvable, rather than reporting
    how many it looked at.

    UPDATE-IF-PRESENT, deliberately NOT an upsert, and this is the whole reason
    the function exists instead of a second call to
    :func:`upsert_request_quota_config`. ``POST /api/budget/sync-from-licenses``
    now fetches every licence regardless of status, so an upsert here would
    manufacture a ``request_quota_config`` row for every revoked licence in the
    registry — 92 of 258 measured 2026-09-08 — none of which ever had one.
    Those rows carry ``quota_monthly``, and ``quota_check`` reads this table as
    the source of truth for a configured limit, so inventing them would invent
    enforcement limits for licences nobody provisioned. Marking only what is
    already mirrored changes exactly the rows whose missing status was the
    defect, and nothing else.

    The row is KEPT rather than deleted. ``delete_absent_quota_configs`` used to
    be the only thing that ever removed a revoked licence's row, and deleting is
    a lossy way to say "revoked": the row carries the enforcement limit, and
    ``quota_check`` falls back to a CALLER-SUPPLIED ``quota_monthly`` when no row
    exists, so the hard delete traded a stale attribution for a fail-open quota.
    A status column lets the mirror say "known, and not active" while keeping the
    limit. Attribution is unaffected either way: ``services.license_resolver``
    filters ``status == 'active'``, so a marked row is not a candidate.

    FLUSH only — the caller owns the transaction boundary, so this composes
    inside the sync-from-licenses transaction alongside its audit writes.
    """
    result = await session.execute(
        update(RequestQuotaConfig)
        .where(
            RequestQuotaConfig.license_id == license_id,
            # No-op guard: without it every already-marked row is rewritten on
            # every reconcile, so `updated_at` churns and the returned count
            # reports "rows examined" rather than "rows transitioned".
            RequestQuotaConfig.status != status,
        )
        .values(status=status, updated_at=_utcnow())
    )
    await session.flush()
    # `AsyncSession.execute` is typed as returning `Result`, but a DML statement
    # always yields a `CursorResult` at runtime, which is where `rowcount` lives.
    # Cast rather than widen the return type: the count is the whole point of the
    # function (see the docstring on transitions-vs-examined).
    return bool(cast("Any", result).rowcount)


class QuotaValueDivergence(NamedTuple):
    """One licence whose mirrored limit disagrees with the upstream plan limit.

    ``mirrored`` is what ``request_quota_config.quota_monthly`` held BEFORE this
    run touched it; ``expected`` is what the licences service's ``plan`` says it
    should be. Both are carried so an operator can tell WHICH WAY the drift goes
    without a second query -- ``expected > mirrored`` is a provisioned limit that
    the plan is about to raise, ``expected < mirrored`` is a plan limit that some
    writer has already overshot.
    """

    license_id: uuid.UUID
    mirrored: int
    expected: int


async def snapshot_quota_config_values(
    session: AsyncSession,
) -> dict[uuid.UUID, int]:
    """Read every mirrored ``quota_monthly``, keyed by licence. READ-ONLY.

    ⛔ MUST be called BEFORE the caller's own upsert loop, and that ordering is
    the whole reason this is a separate function instead of a query inside
    :func:`record_quota_config_value_drift`.
    ``routers.budget.sync_from_licenses`` upserts every licence's plan quota and
    only THEN measures drift, so a detector that read the table itself at
    measurement time would be reading rows the same run had just overwritten
    with the very values it was about to compare them against -- it would
    report 0 divergence unconditionally, forever, which is exactly the
    "instrument that grades its own subject" failure the id-set-only detector
    already shipped once.

    Distinct from :func:`resolve_quota_limits`, which is a bounded
    ``WHERE license_id IN`` lookup on a request path; this is a deliberate full
    scan of a small operator-facing mirror table (183 rows on prod 2026-09-02).
    """
    result = await session.execute(
        select(RequestQuotaConfig.license_id, RequestQuotaConfig.quota_monthly)
    )
    return {row.license_id: row.quota_monthly for row in result.all()}


def record_quota_config_value_drift(
    *,
    expected_quota_monthly: Mapping[uuid.UUID, int],
    mirror_quota_before: Mapping[uuid.UUID, int],
    sample_limit: int = 10,
) -> tuple[int, list[QuotaValueDivergence]]:
    """Compare the mirrored LIMIT against the upstream plan limit, READ-ONLY.

    ⭐ THE TWO SIDES ARE INDEPENDENT, and this function takes BOTH as arguments
    precisely so it cannot quietly source them from one place:

    * ``expected_quota_monthly`` -- ``plan.quota_monthly`` per licence, from the
      caller's live ``GET {licenses_url}/internal/licenses`` fetch. That is the
      LICENCES service's own Postgres, reached over HTTP.
    * ``mirror_quota_before`` -- ``request_quota_config.quota_monthly`` from
      BILLING's Postgres, snapshotted by :func:`snapshot_quota_config_values`
      before this run wrote anything.

    There is no dblink between the two databases, so that pairing is the only
    truthful comparison available. It is also why this function takes no
    ``AsyncSession`` AT ALL: it is structurally incapable of querying the table
    it audits, so it cannot degenerate into the self-grading shape that made
    "164 rows, 0 divergence" a statement about the detector rather than about
    the data.

    WHY THE ID-SET DETECTOR COULD NOT SEE THIS
    ------------------------------------------
    :func:`record_quota_config_drift` compares WHICH licences are present. Three
    writers upsert on the ``license_id`` primary key and all three assign
    ``quota_monthly`` unconditionally -- deliberately, see that function's
    docstring for why gating it was tried and refuted. A plan limit of 1,000,000 landing
    on top of a provisioned 1,000 changes no id, adds no row and removes none --
    every set comparison stays empty while the enforced limit has moved by three
    orders of magnitude.

    SCOPE: the INTERSECTION of the two keysets only. A licence in ``expected``
    with no mirror row is already counted by ``quota_config_missing_total``, and
    a mirror row for a licence upstream no longer returns is already counted by
    ``quota_config_orphan_total``; comparing values there would double-count one
    defect under two gauges. Callers must also OMIT licences whose plan carries
    no ``quota_monthly`` (unlimited / absent) rather than coercing them to 0 --
    a 0 limit is BLOCK ALL on this table (see :func:`check_quota`), so coercing
    would manufacture drift against a licence nobody configured.

    Returns ``(divergent_count, sample)``. The sample is bounded by
    ``sample_limit`` and sorted by licence id -- DETERMINISTIC on purpose, so two
    consecutive runs over the same drift report the same rows and an operator can
    tell a spreading problem from dict iteration order.
    """
    divergent = [
        QuotaValueDivergence(
            license_id=license_id,
            mirrored=mirror_quota_before[license_id],
            expected=expected_quota_monthly[license_id],
        )
        for license_id in sorted(
            mirror_quota_before.keys() & expected_quota_monthly.keys(), key=str
        )
        if mirror_quota_before[license_id] != expected_quota_monthly[license_id]
    ]

    observability_metrics.quota_config_value_drift.set(len(divergent))

    if divergent:
        # WARNING, not debug: this is the only place the actual VALUES surface.
        # The gauge can say "12 licences disagree"; it can never say which, nor
        # in which direction, and the mirror is overwritten moments later by the
        # caller's own upsert loop -- so if this line is not emitted the evidence
        # is gone until the next operator-triggered sync.
        logger.warning(
            "quota_config_value_drift",
            divergent=len(divergent),
            compared=len(mirror_quota_before.keys() & expected_quota_monthly.keys()),
            sample=[
                {
                    "license_id": str(d.license_id),
                    "mirrored": d.mirrored,
                    "expected": d.expected,
                }
                for d in divergent[:sample_limit]
            ],
        )

    return len(divergent), divergent[:sample_limit]


async def record_quota_config_drift(
    session: AsyncSession,
    *,
    known_license_ids: set[uuid.UUID],
    active_license_ids: set[uuid.UUID],
    expected_quota_monthly: Mapping[uuid.UUID, int] | None = None,
    mirror_quota_before: Mapping[uuid.UUID, int] | None = None,
    sample_limit: int = 10,
) -> tuple[int, int]:
    """Measure and publish how far the mirror has drifted, READ-ONLY.

    Deletes nothing and marks nothing -- it only computes a set comparison
    against the mirror's CURRENT rows and calls
    ``observability.metrics.quota_config_orphan_total`` /
    ``quota_config_missing_total``. Callers pass the SAME
    ``known_license_ids`` / ``active_license_ids`` sets that
    ``routers.budget.sync_from_licenses`` already built from that run's live
    licences fetch -- there is no dblink between billing's and licences'
    Postgres databases, so this function must never be called with anything
    other than a genuinely-just-fetched pair, or the gauges would silently
    compare the mirror to itself.

    ⛔ IT COMPARED ID SETS ONLY, and a previous audit round read that as a
    verdict on the DATA: "164 rows, 0 divergence, table healthy". It was a
    verdict on the DETECTOR. Three writers upsert on the ``license_id`` primary
    key, so one silently replacing another's ``quota_monthly`` moves no id and
    was invisible here. Passing ``expected_quota_monthly`` AND
    ``mirror_quota_before`` adds that comparison via
    :func:`record_quota_config_value_drift` -- see its docstring for why the two
    sides are independent.

    ⚠️ The value comparison is OPT-IN and, when the two mappings are not
    supplied, ``quota_config_value_drift`` is left strictly UNTOUCHED rather than
    zeroed. The gauge is born at ``-1`` = never measured (see its HELP text): a
    caller that does not measure must not be able to publish a reassuring 0, and
    must not be able to overwrite a real measurement another call in the same run
    already published. The existing gauges keep their exact meaning -- neither is
    repurposed, and nothing here is conditional on the new arguments.

    ⛔ ``mirror_quota_before`` must come from
    :func:`snapshot_quota_config_values` called BEFORE the caller's upsert loop.
    Supplying one mapping without the other raises rather than silently measuring
    nothing: a half-configured detector that reports "no drift" is the failure
    mode this whole function exists to stop.

    Returns ``(orphan_count, missing_count)`` so the caller can fold the same
    numbers into its own audit/response payload without a second query. The
    return shape is DELIBERATELY unchanged -- the value-drift count comes back
    from :func:`record_quota_config_value_drift`, which the caller should invoke
    directly when it wants the number for its response payload, rather than from
    a widened tuple that would break every existing two-value unpack.
    """
    if (expected_quota_monthly is None) != (mirror_quota_before is None):
        raise ValueError(
            "expected_quota_monthly and mirror_quota_before must be supplied "
            "together: one without the other can only measure nothing while "
            "appearing to measure value drift"
        )

    mirror_ids = set((await session.execute(select(RequestQuotaConfig.license_id))).scalars().all())
    absent = mirror_ids - known_license_ids
    non_active = (mirror_ids & known_license_ids) - active_license_ids
    missing = active_license_ids - mirror_ids

    observability_metrics.quota_config_orphan_total.labels(reason="absent").set(len(absent))
    observability_metrics.quota_config_orphan_total.labels(reason="non_active").set(len(non_active))
    observability_metrics.quota_config_missing_total.set(len(missing))

    if expected_quota_monthly is not None and mirror_quota_before is not None:
        record_quota_config_value_drift(
            expected_quota_monthly=expected_quota_monthly,
            mirror_quota_before=mirror_quota_before,
            sample_limit=sample_limit,
        )

    return len(absent) + len(non_active), len(missing)


def record_quota_config_sync_success() -> None:
    """Mark that a sync-from-licenses run completed with no fetch error.

    Sets the raw Unix timestamp, not an elapsed age -- see the gauge's HELP
    text in observability.metrics for why an age would need a ticking loop
    this endpoint (operator-triggered only) deliberately does not have.
    """
    observability_metrics.quota_config_sync_last_success_timestamp_seconds.set(
        _utcnow().timestamp()
    )


async def resolve_quota_limits(
    license_ids: list[uuid.UUID],
    session: AsyncSession,
) -> dict[uuid.UUID, int]:
    """Load durable request-quota limits for a batch of licenses.

    The durable ``request_quota_config`` row is the source of truth for a
    license's configured monthly quota; callers fall back to the licenses-service
    ``plan.quota_monthly`` only when no row exists. One ``WHERE license_id IN``
    query, not N round-trips.
    """
    if not license_ids:
        return {}
    stmt = select(RequestQuotaConfig.license_id, RequestQuotaConfig.quota_monthly).where(
        RequestQuotaConfig.license_id.in_(license_ids)
    )
    result = await session.execute(stmt)
    return {row.license_id: row.quota_monthly for row in result.all()}


async def check_quota(
    license_id: uuid.UUID,
    plan_id: uuid.UUID | None,
    quota_monthly: int | None,
    session: AsyncSession,
) -> dict:
    """Check monthly request-quota status for one license.

    Usage is the license's month-to-date ``metering_monthly_usage.request_count``
    (see :func:`_get_db_usage`); ``plan_id`` is echoed in the response only.

    Fails CLOSED at this layer: a DB error propagates and
    ``main._sqlalchemy_exception_handler`` turns it into 503. Never default
    ``used`` to 0 on an error — that would report every licence under quota for
    as long as Postgres was unreadable.
    """
    used = await _get_db_usage(license_id, current_month_start(), session)

    # Semantics of the limit:
    #   quota_monthly is None  -> UNLIMITED  (remaining=None, never over_quota)
    #   quota_monthly == 0     -> BLOCK ALL  (remaining=0 or negative -> over_quota)
    #   quota_monthly > 0      -> normal cap
    # A stored 0 is a deliberate zero-allowance / suspended plan, NOT unlimited;
    # unlimited is ONLY the None (no-config) branch. Because over_quota keys off
    # ``remaining <= 0``, a 0 limit reports over_quota=True even at 0 usage.
    remaining = (quota_monthly - used) if quota_monthly is not None else None
    over_quota = remaining is not None and remaining <= 0

    return {
        "license_id": str(license_id),
        "plan_id": str(plan_id) if plan_id is not None else None,
        "quota_monthly": quota_monthly,
        "used_this_month": used,
        "remaining": remaining,
        "over_quota": over_quota,
    }


async def _get_db_usage(
    license_id: uuid.UUID,
    month_start: date,
    session: AsyncSession,
) -> int:
    """Monthly request count for one license — the only usage counter.

    Sums every ``metering_monthly_usage`` row for (license_id, month_start),
    collapsing the subject and plan grains: the row grain is
    ``uq_monthly_usage (license_id, subject_id, plan_id, month_start)`` and
    ``event_flusher`` upserts one row per subject. ``scalar_one_or_none()`` over
    that grain used to raise ``MultipleResultsFound`` (a 503 blamed on a healthy
    DB) or silently return one subject's count as the license total.

    ``plan_id`` is deliberately not a predicate: a mid-month plan change splits a
    license's rows, and filtering by plan would drop the other half.
    """
    stmt = select(func.coalesce(func.sum(MeteringMonthlyUsage.request_count), 0)).where(
        MeteringMonthlyUsage.license_id == license_id,
        MeteringMonthlyUsage.month_start == month_start,
    )
    result = await session.execute(stmt)
    return int(result.scalar_one())
