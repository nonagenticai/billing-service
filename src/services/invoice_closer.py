"""Leader-elected background loop that drives the monthly invoice close.

The actual close logic lives in :func:`services.invoices.close_due_periods` --
this module is only the loop that decides WHEN to call it, on which replica,
and publishes what happened.

WHY THE LOCK RIDES THE SESSION-MODE POOLER, NOT ``DATABASE_URL``
------------------------------------------------------------------
Short version of ``services.leader_election``'s module docstring -- read that
for the full story. ``DATABASE_URL`` points at the TRANSACTION-mode pgbouncer,
which rebinds a different Postgres backend per transaction. A Postgres
advisory lock is SESSION-scoped, so a lock taken over that pooler is silently
released on the very next checkout -- every replica then sees
``pg_try_advisory_lock -> true`` and believes itself leader. For this loop
that is not just a duplicated log line: it is two replicas independently
calling :func:`~services.invoices.close_due_periods` for the same period,
each drawing its own number from the gapless invoice-number counter and
minting two invoices for one tenant-month. ``Settings.resolve_leader_election_dsn``
is the one path that resolves to the SESSION-mode pooler
(``pgbouncer-session...``), so that -- never ``settings.database_url`` -- is
what :class:`~services.leader_election.AsyncLeaderElection` must be built
from here.

SHAPE OF THE LOOP
------------------
Copied from ``services.aggregator.run_rollup_loop``, not
``services.budget_watcher.run_watcher_loop``. The two sibling loops differ in
two load-bearing ways and this one follows the rollup loop on both:

* ``invoice_close_is_leader`` is set in a ``try/finally`` around
  :meth:`~services.leader_election.AsyncLeaderElection.is_leader`, so a
  raising leadership check still publishes ``0`` rather than leaving the
  gauge on its last (possibly stale ``1``) value.
* A leader that fails ``MAX_CONSECUTIVE_INVOICE_CLOSE_FAILURES`` cycles in a
  row hands the advisory lock back (see ``_cycle_failed``) instead of holding
  it forever. ``is_leader()`` NEVER raises -- it catches its own errors and
  returns ``False`` -- so a wedged DB connection reads as "not leader", which
  is exactly why this counts CONSECUTIVE cycle failures rather than trusting
  that boolean: a replica that can no longer reach Postgres would otherwise
  sit there reporting itself a healthy follower forever while nothing closes
  a single period, fleet-wide.

AUDIT PROVENANCE IS PART OF THE CLOSE, NOT DECORATION
------------------------------------------------------
Every invoice this loop mints gets an ``audit_log`` row, written through
:func:`services.audit.record` -- the SAME function and the SAME
``action`` / ``resource_type`` / ``after`` shape the three HTTP routes use
(``routers/billing.py:505`` close, ``:583`` issue, ``:647`` void). Before
this, ``close_due_periods`` was the only invoice-minting path in the service
that wrote NO audit row at all: the actor survived solely as the string
``"close_due_periods"`` in ``billing_invoices.created_by``. The whole reason
money writes are routed through the API rather than SQL is that the audit log
carries provenance for every pricing row, and the automated path -- the one
that will mint nearly every invoice once ``INVOICE_CLOSE_ENABLED`` is armed --
bypassed exactly that.

⚠️ THE AUDIT ROW CANNOT RIDE THE INVOICE'S OWN TRANSACTION FROM HERE, and that
is a real difference from the HTTP sites, not an oversight. ``close_due_periods``
owns a session PER TENANT and commits it itself (``services/invoices.py:1687``);
by the time it returns, every invoice in ``outcome.created`` is already durable.
So this module can only write the audit rows in a SECOND transaction, and a
crash in the gap leaves an invoice with no audit row. The honest home for this
write is inside ``close_due_periods``'s per-tenant ``async with`` block, before
its ``commit()`` -- same-transaction, per-invoice, atomic. Moving it there is a
follow-up in ``services/invoices.py``; this module is where the provenance can
be added without touching the close logic itself.

⛔ The audit write therefore must NOT be wrapped in a swallowing
``try/except`` the way ``_measure_invoice_close_lag`` deliberately is. At the
HTTP sites an ``audit.record`` failure propagates out of the handler, the route
never reaches its ``commit()``, and the invoice write is ROLLED BACK -- audit
failure aborts the operation. This loop cannot roll back an already-committed
invoice, so the closest faithful behaviour is: count it on
``invoice_close_failures_total{reason="audit_error"}``, log the invoice ids /
numbers / tenants at ERROR (so the provenance survives in the log even when the
row does not), and let the exception reach the loop's own ``except`` arms, where
it fails the CYCLE and counts toward ``MAX_CONSECUTIVE_INVOICE_CLOSE_FAILURES``.
A close that succeeds while its audit row silently fails is the defect shape
this is here to prevent, so silence is the one outcome that is not allowed.

THE GAUGE PAIR
---------------
``invoice_close_lag_days`` / ``invoice_close_lag_timestamp_seconds`` are a
pair, and the pairing is the whole point. A ``prometheus_client`` ``Gauge``
with no labels is exported at ``0.0`` from the moment it is constructed --
before this loop has ever run a single tick. That makes "every period is
closed" and "this replica has never measured" the SAME zero. The timestamp
gauge is what tells them apart: it is written ONLY immediately after
``_measure_invoice_close_lag`` returns successfully, never on a failure path,
never at import, and never "to keep the series fresh". A stale timestamp
next to an unmoving ``lag_days`` is an honestly unreadable state (unknown);
setting the timestamp on any other path would make that state
indistinguishable from a real, current, healthy measurement -- which
destroys the only signal this pair exists to carry.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import structlog
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import BillingInvoice
from observability import metrics as observability_metrics
from services import audit
from services.invoices import close_due_periods, find_close_candidates
from services.leader_election import INVOICE_LOCK_KEY, AsyncLeaderElection
from settings import Settings

if TYPE_CHECKING:  # pragma: no cover - typing only
    from fastapi import Request

logger = structlog.stdlib.get_logger(__name__)

# Consecutive failed LEADER cycles before the advisory lock is handed back to
# whichever other replica can acquire it next. Same constant/shape as
# services.aggregator.MAX_CONSECUTIVE_ROLLUP_FAILURES and the same reasoning:
# holding leadership through an unbounded run of failures would freeze
# invoice closing fleet-wide behind a single wedged pod that still reports
# Ready (``main._log_task_death`` only logs; ``/health/ready`` never looks at
# background tasks).
MAX_CONSECUTIVE_INVOICE_CLOSE_FAILURES = 3

# Bound on how far back services.invoice_closer will SCAN when measuring
# invoice_close_lag_days. close_due_periods only ever acts on the single most
# recently completed calendar month (see its own docstring -- it has no
# catch-up path), so an outage spanning more than one billing cycle leaves
# every OLDER unclosed month permanently invisible to the ACTION this loop
# takes. This census exists only to make that backlog OBSERVABLE; nothing
# here repairs it -- closing an old period this loop cannot see repaired
# rollup data for would risk drawing an invoice number over usage nobody can
# still vouch for is complete, the same "report, do not repair" rule
# services.rollup_engine.daily_rollup_date_gaps documents for the rollup
# side. 24 months is generous for a monthly cycle; a real gap deeper than
# that reads as "at least this old", not the true age.
_LAG_CENSUS_MAX_PERIODS = 24


# ---------------------------------------------------------------------------
# Audit provenance for the non-HTTP close path. See the module docstring.
# ---------------------------------------------------------------------------

#: ``audit_log.actor_sub`` for an invoice minted by this loop.
#:
#: ⚠️ The SAME literal ``close_due_periods`` passes as its ``actor=`` argument
#: to ``close_period`` (``services/invoices.py:1686``), which is what lands in
#: ``billing_invoices.created_by`` / ``issued_by``. Keeping them identical is
#: the point: the audit row's actor JOINS to the invoice's own column, so one
#: act has one name. It is duplicated rather than imported because that value
#: is an inline literal in ``services/invoices.py`` with no exported constant
#: to import; promoting it to one there (and importing it here) is the right
#: follow-up, and ``test_loop_audit_actor_matches_invoice_created_by`` is the
#: guard that catches the two drifting apart in the meantime.
AUDIT_ACTOR_SUB = "close_due_periods"

#: ``audit_log.actor_role`` for an invoice minted by this loop.
#:
#: ⚠️ There is NO pre-existing role literal for THIS channel; the convention
#: being followed is the SHAPE of ``auth_deps.py:363`` --
#: ``_stash_actor(request, sub="n8n-internal", role="internal-secret")`` -- which
#: establishes that a non-Keycloak caller is recorded with a literal naming the
#: CHANNEL it came through, not a realm role. (``role="tenant"`` at
#: ``auth_deps.py:207`` is the second instance.) So this is not a Keycloak realm
#: role and must never be confused with one: the loop presents no token at all,
#: it holds a Postgres advisory lock. Stamping ``billing-internal`` here -- the
#: role the billing-service SA really does present on the pricing-seed CronJob
#: path -- would claim a verified Keycloak identity that was never verified AND
#: make the loop's rows indistinguishable from that SA's. ``leader-loop`` names
#: what actually authorised the write.
AUDIT_ACTOR_ROLE = "leader-loop"

#: ``audit_log.action`` -- byte-identical to ``routers/billing.py:508``.
#: Deliberately NOT a loop-specific action: the ACT is the same act, and
#: splitting the vocabulary would mean any query over "who closed invoices"
#: silently misses every automated close. The actor columns are what tell the
#: two paths apart.
AUDIT_ACTION_CLOSE = "billing.invoice.close"


class _LoopAuditRequest:
    """The minimum surface :func:`services.audit.record` reads off a ``Request``.

    ``record`` is typed on ``Request`` because every other caller is an HTTP
    handler, but it touches exactly two attributes:
    ``request.state.audit_actor`` and ``request.state.request_id``
    (``services/audit.py:90-93``). Synthesising those is deliberate and is the
    cheaper of the two available options: the alternative -- hand-building an
    ``AuditLog`` row in this module -- would make ``services.audit.record`` stop
    being the single writer of the audit schema, and the loop's row could then
    drift in shape from the three route rows without anything noticing. That
    drift is the exact failure ``test_loop_close_audit_row_matches_http_shape``
    exists to prevent, and a second INSERT site would defeat it at the source.

    ``request_id`` carries a per-SWEEP ``uuid7`` minted the same way
    ``middleware/request_id.py:30`` mints one for a request with no inbound
    ``X-Request-ID``. It is a correlation id, not a claim that an HTTP request
    happened: it groups every audit row written by one tick.
    """

    __slots__ = ("state",)

    def __init__(self, request_id: str) -> None:
        self.state = SimpleNamespace(
            audit_actor={"sub": AUDIT_ACTOR_SUB, "role": AUDIT_ACTOR_ROLE},
            request_id=request_id,
        )


async def _record_close_audit(
    session_factory: async_sessionmaker[AsyncSession],
    invoices: Sequence[BillingInvoice],
) -> str | None:
    """Write one ``audit_log`` row per invoice this sweep actually MINTED.

    ``invoices`` is ``CloseDueOutcome.created`` and nothing else. ``unchanged``
    is deliberately excluded: those periods were closed by an earlier sweep and
    already carry their row, so auditing them would re-record the same act on
    every tick -- the same shape as the phantom-``issued`` counter defect
    ``TestCloseOutcomeAccounting`` was written for, except written into an
    append-only table where it cannot be taken back.

    Returns the sweep's correlation id, or ``None`` when there was nothing to
    audit. ⛔ Raises on failure -- see the module docstring for why this one
    must not be swallowed.
    """
    if not invoices:
        return None

    sweep_id = str(uuid.uuid7())
    audit_request = cast("Request", _LoopAuditRequest(sweep_id))
    try:
        # ONE session and ONE transaction for the whole sweep's rows. The
        # invoices are already committed individually, so there is no per-tenant
        # isolation left to protect here; all-or-nothing across the batch is
        # strictly better than a partial write, because a raise then leaves NO
        # rows rather than an arbitrary prefix, and the ERROR log below names
        # every invoice that needs one.
        async with session_factory() as session:
            for invoice in invoices:
                await audit.record(
                    session,
                    audit_request,
                    action=AUDIT_ACTION_CLOSE,
                    resource_type="invoice",
                    resource_id=str(invoice.id),
                    tenant_id=invoice.tenant_id,
                    # Same two keys, same order, as routers/billing.py:512.
                    after={
                        "status": invoice.status,
                        "invoice_number": invoice.invoice_number,
                    },
                )
            # ⛔ `audit.record` only FLUSHES, by its own documented contract
            # ("the route handler is responsible for committing"), and
            # AsyncSession.__aexit__ CLOSES, which rolls back. Without this
            # line this function looks like it worked and writes nothing --
            # the precise failure mode routers/billing.py:515 documents at
            # length for the HTTP close.
            await session.commit()
    except Exception:
        # Counter FIRST: structlog renders inside the logger call, so a broken
        # handler or a bad bound value raises THERE, and a raise before the
        # increment would cost the one signal an operator triages from. Same
        # reasoning as `_log_loop_failure`, which is why the log is suppressed.
        observability_metrics.invoice_close_failures_total.labels(reason="audit_error").inc()
        with contextlib.suppress(Exception):
            logger.error(
                "invoice_close_audit_write_failed",
                sweep_id=sweep_id,
                invoice_ids=[str(inv.id) for inv in invoices],
                invoice_numbers=[inv.invoice_number for inv in invoices],
                tenant_ids=[inv.tenant_id for inv in invoices],
                exc_info=True,
            )
        raise
    return sweep_id


def _step_back_one_month(period_start: date) -> tuple[date, date]:
    """Return the ``(start, end)`` of the calendar month immediately before
    ``period_start``. ``end`` of the result is exactly ``period_start``
    (half-open interval, matching ``BillingInvoice.period_end``)."""
    end = period_start
    start = date(end.year - 1, 12, 1) if end.month == 1 else date(end.year, end.month - 1, 1)
    return start, end


def _last_completed_month(now: datetime) -> tuple[date, date]:
    """``(period_start, period_end)`` of the last COMPLETED calendar month at
    ``now`` -- the one period ``close_due_periods`` will act on this tick.

    Deliberately duplicated from (not imported off of)
    ``services.invoices._previous_month``: that name is module-private, and
    this loop's only load-bearing dependency on ``services.invoices`` should
    be its one public entry point, ``close_due_periods``. The two-line date
    computation is cheap to keep in step; a private cross-module import is
    not.
    """
    return _step_back_one_month(date(now.year, now.month, 1))


async def _measure_invoice_close_lag(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    settings: Settings,
    now: datetime,
) -> int:
    """Age, in days, of the OLDEST complete period that is already past its
    settle window (``period_end + settings.invoice_close_lag_days``) but
    still has at least one active tenant with no ``billing_invoices`` row for
    it. Returns ``0`` when every scanned period is fully closed -- the
    healthy value.

    "Active tenant" for a period is defined exactly the way
    ``close_due_periods`` defines it -- by CALLING THE SAME FUNCTION,
    ``services.invoices.find_close_candidates``, rather than restating it.
    ``close_due_periods`` itself cannot serve this: it only ever reports the
    invoices it closed for the ONE period it was given, never a per-tenant
    picture of an arbitrary older period. So the census still drives the loop
    below, but the DEFINITION of who belongs in a period is imported.

    ⭐ WHY IT IS IMPORTED AND NO LONGER COPIED. The copy said "any tenant with a
    ``billing_cost_daily`` or ``billing_metering_daily`` row in the period" --
    identical to the close's own definition at the time, and identically blind:
    neither consulted ``tenant_access_charge``, so a tenant on a flat access
    charge with no usage was invisible to BOTH. That is the worst possible
    arrangement for a backlog gauge, because the instrument shared the subject's
    defect: the unbilled tenant-period was not merely uncounted, it could not be
    counted, and this gauge read a flat healthy 0 while the customer went
    unbilled every month. Two copies that agree today are exactly how that
    happens again; a single query cannot drift from itself.

    ⚠️ The imported name is module-private-ADJACENT but genuinely public, unlike
    ``_previous_month`` above, which is why this import is fine where that one is
    not: the candidate definition is the close's contract with anything that
    audits it, and there is no second honest way to spell it.
    """
    period_start, period_end = _last_completed_month(now)
    oldest_gap_end: date | None = None
    # Periods strictly before the floor are not billable, so an uncovered one is
    # not a backlog -- see Settings.invoice_first_billable_period. Read once: the
    # same value ``close_due_periods`` floors its own action on.
    first_billable = settings.invoice_first_billable_period_start

    for _ in range(_LAG_CENSUS_MAX_PERIODS):
        if first_billable is not None and period_start < first_billable:
            # The scan only ever walks BACKWARDS, so every remaining period is
            # older still and equally below the floor.
            break

        settle_date = period_end + timedelta(days=settings.invoice_close_lag_days)
        if now.date() < settle_date:
            # Only ever true on the first (newest) iteration: every older
            # period has an EARLIER period_end, so its own settle_date is
            # earlier too and this branch will not protect it. Move on and
            # keep scanning -- an older period can independently already be
            # overdue even though the newest one is not yet due.
            period_start, period_end = _step_back_one_month(period_start)
            continue

        async with session_factory() as session:
            # ⛔ THE SAME FUNCTION THE CLOSE ITSELF ASKS, not a restatement of it.
            # See this function's docstring for what the copy this replaced could
            # not see. It counts a tenant as active on an access charge alone, so
            # a term-only tenant-period that was never closed now surfaces here as
            # lag -- which is the only place it can surface, since
            # ``close_due_periods`` acts on the last completed month only.
            active = set(
                (
                    await find_close_candidates(
                        session, period_start=period_start, period_end=period_end
                    )
                ).tenant_ids
            )
            if not active:
                # No activity from ANY tenant this specific month does not
                # imply older months are also empty (a tenant can resume
                # after a dormant month), so keep scanning rather than
                # breaking early.
                period_start, period_end = _step_back_one_month(period_start)
                continue

            closed_tenants = {
                r.tenant_id
                for r in (
                    await session.execute(
                        select(BillingInvoice.tenant_id).where(
                            BillingInvoice.period_start == period_start,
                            BillingInvoice.period_end == period_end,
                            BillingInvoice.tenant_id.in_(active),
                            # ⚠️ A VOIDED invoice is not coverage: voiding
                            # reopens the tenant-period, and this census must
                            # see that gap rather than count the dead row as a
                            # close. The status set is spelled POSITIVELY,
                            # matching services.invoices.find_open (and through
                            # it ux_billing_invoices_open_period) -- under
                            # `!= 'void'` a status added later would silently
                            # join this set and the two would start disagreeing
                            # about what "open" means.
                            # ⛔ Nothing here repairs it: close_due_periods only
                            # ever acts on the LAST completed month, so a period
                            # voided after that tick surfaces here as lag a
                            # human has to close by hand -- which is precisely
                            # the signal this gauge exists to carry.
                            BillingInvoice.status.in_(("draft", "issued")),
                        )
                    )
                ).all()
            }

        if active - closed_tenants:
            oldest_gap_end = period_end

        period_start, period_end = _step_back_one_month(period_start)

    if oldest_gap_end is None:
        return 0
    return (now.date() - oldest_gap_end).days


def _log_loop_failure(event: str, level: str = "error") -> None:
    """Emit a loop-failure log without letting the LOGGER take the loop down.

    Same rationale and shape as ``services.aggregator._log_loop_failure``:
    structlog renders inside this call, so a broken handler or a bad bound
    value raises HERE -- inside the loop's own except arm, where nothing is
    left to catch it -- and would otherwise escape the ``while`` and kill the
    task.
    """
    with contextlib.suppress(Exception):
        getattr(logger, level)(event, exc_info=True)


async def run_invoice_close_loop(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    stop_event: asyncio.Event,
) -> None:
    """Periodically close the last completed billing month, leader-elected.

    The leader waits the full ``settings.invoice_close_interval_seconds``
    between ticks; a follower (or a cycle that raised before proving
    leadership) waits only ``settings.leader_retry_interval_seconds`` before
    re-racing for the lock -- same roll-window fix as
    ``services.aggregator.run_rollup_loop`` and
    ``services.budget_watcher.run_watcher_loop``: a follower that idled a
    full interval would leave invoice closing leaderless for up to that long
    after anything dropped the leader's session.
    """
    direct_dsn = settings.resolve_leader_election_dsn()
    election = AsyncLeaderElection(direct_dsn, INVOICE_LOCK_KEY)

    # Clamped to the loop's own cadence: a follower must never wait LONGER
    # than the leader's interval, or it would re-race for the lock more
    # slowly than the leader ticks.
    retry_seconds = min(
        settings.leader_retry_interval_seconds, settings.invoice_close_interval_seconds
    )
    # Starts at the short retry, not the full interval: a freshly started
    # replica has not proven leadership either, and must not sit out a full
    # cycle before even looking at the lock.
    wait_seconds = retry_seconds

    consecutive_failures = 0

    def _cycle_ok() -> None:
        nonlocal consecutive_failures
        consecutive_failures = 0

    async def _cycle_failed(reason: str) -> None:
        nonlocal consecutive_failures, wait_seconds
        consecutive_failures += 1
        observability_metrics.invoice_close_failures_total.labels(reason=reason).inc()
        if consecutive_failures < MAX_CONSECUTIVE_INVOICE_CLOSE_FAILURES:
            return
        # Wedged: give the lock back so a replica that is NOT wedged can
        # close periods. Suppressed because this runs inside the loop's own
        # except arm -- a raise here would escape the `while` and kill the
        # task, the exact failure the broad arm exists to prevent.
        # is_leader() re-acquires on the next cycle if nobody else took it.
        with contextlib.suppress(Exception):
            await election.release()
        _log_loop_failure("invoice_close_leadership_handover", level="warning")
        consecutive_failures = 0
        wait_seconds = retry_seconds

    try:
        while not stop_event.is_set():
            try:
                # Interruptible wait: shutdown must not have to wait out a
                # full interval.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
                if stop_event.is_set():
                    break

                # Not yet proven leader for THIS cycle -> if anything below
                # raises, retry soon rather than parking for a full interval.
                wait_seconds = retry_seconds

                leader = False
                try:
                    leader = await election.is_leader()
                finally:
                    observability_metrics.invoice_close_is_leader.set(1 if leader else 0)

                if not leader:
                    continue

                wait_seconds = settings.invoice_close_interval_seconds

                now = datetime.now(UTC)
                _period_start, period_end = _last_completed_month(now)
                settle_date = period_end + timedelta(days=settings.invoice_close_lag_days)

                if now.date() < settle_date:
                    # The candidate period has not sat through its settle
                    # window yet (late rollup rows may still land) -- nothing
                    # eligible to close this tick.
                    observability_metrics.invoices_closed_total.labels(status="skipped").inc()
                else:
                    outcome = await close_due_periods(session_factory, settings, now=now)
                    for invoice in outcome.created:
                        # close_due_periods always calls close_period with its
                        # default issue_immediately=True, so every returned
                        # invoice's status is currently always "issued" --
                        # read off the row rather than hardcoding that, so
                        # this stays correct if that default ever changes.
                        status_label = invoice.status if invoice.status else "issued"
                        observability_metrics.invoices_closed_total.labels(
                            status=status_label
                        ).inc()
                    if outcome.unchanged:
                        # ⚠️ Its OWN label, never "issued": these periods were
                        # already closed before this sweep, so a re-observation
                        # drew no number from the gapless invoice-number series
                        # and minted nothing. Counting them as "issued" would
                        # re-report every past close on every tick.
                        observability_metrics.invoices_closed_total.labels(status="unchanged").inc(
                            len(outcome.unchanged)
                        )
                    if outcome.refused:
                        observability_metrics.invoice_close_failures_total.labels(
                            reason="tenant_refused"
                        ).inc(outcome.refused)
                    if outcome.failed:
                        observability_metrics.invoice_close_failures_total.labels(
                            reason="tenant_error"
                        ).inc(outcome.failed)
                    if outcome.skipped:
                        # ⚠️ NOT on invoices_closed_total{status="skipped"}, which
                        # already means something else: "the period is not yet past
                        # its settle window". That one is a per-CYCLE fact about
                        # time; this is a per-TENANT fact about money. Reusing the
                        # label would make a month in which nothing was eligible
                        # indistinguishable from a month in which ten internal cost
                        # centres correctly charged nothing.
                        #
                        # ⚠️ The per-tenant increment already happened inside
                        # close_period (it is the only place that knows WHY), so
                        # this loop does NOT re-increment the reason counter — it
                        # would double every skip. The outcome count is logged in
                        # the sweep census instead; this arm exists so the loop's
                        # own log carries it too.
                        logger.info(
                            "invoice_close_cycle_skipped_tenants",
                            skipped=outcome.skipped,
                        )

                    # ⛔ PROVENANCE, NOT OBSERVABILITY -- and deliberately NOT
                    # inside a swallowing try/except (contrast the lag census
                    # below, which is). A raise here reaches the loop's own
                    # except arms and fails the CYCLE. See the module docstring.
                    #
                    # ⚠️ AFTER the counters above, on purpose: every invoice in
                    # `outcome.created` is ALREADY COMMITTED by the time
                    # close_due_periods returns, so those increments describe a
                    # durable fact and must be published whether or not the
                    # audit write then succeeds. Ordering them the other way
                    # would make an audit failure also silently under-report
                    # real closes on `invoices_closed_total`, i.e. lose two
                    # records instead of one.
                    await _record_close_audit(session_factory, outcome.created)

                # Measurement, not action -- must never be able to abort the
                # close above, and its own failure must not cost the cycle
                # its "ok" status. Same rule as run_rollup_loop's
                # daily_rollup_lag_days / run_attribution_audit calls.
                try:
                    lag_days = await _measure_invoice_close_lag(
                        session_factory, settings=settings, now=now
                    )
                except Exception:
                    logger.warning("invoice_close_lag_scan_failed", exc_info=True)
                else:
                    # The pair: value THEN timestamp, and only on this
                    # success path -- see the module docstring for why.
                    # ⚠️ PER-REPLICA LAST MEASUREMENT, written only while this
                    # replica is leader. Nothing clears it on demotion, so an
                    # ex-leader keeps exporting the final value it measured for
                    # the rest of its process lifetime. Any alert over this
                    # gauge must therefore gate on
                    # `billing_invoice_close_is_leader == 1` -- a bare max()
                    # across replicas can read a stale follower's frozen figure
                    # as the fleet's current lag.
                    observability_metrics.invoice_close_lag_days.set(lag_days)
                    observability_metrics.invoice_close_lag_timestamp_seconds.set(time.time())

                _cycle_ok()

            except asyncio.CancelledError:
                break
            except SQLAlchemyError:
                await _cycle_failed("infra_error")
                _log_loop_failure("invoice_close_error_infra", level="warning")
            except Exception:
                # Deliberately broad, same reasoning as run_rollup_loop and
                # run_watcher_loop: one bad cycle (a bad licences response
                # shape, a serialization bug, anything not enumerated) must
                # cost one cycle, not the whole loop for the process
                # lifetime.
                await _cycle_failed("unexpected_error")
                _log_loop_failure("invoice_close_error_unexpected")
    finally:
        await election.release()
