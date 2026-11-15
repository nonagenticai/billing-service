#!/usr/bin/env python3
"""Dry-run the monthly invoice close: run the REAL logic, then throw it away.

WHY THIS EXISTS
---------------
``INVOICE_CLOSE_ENABLED`` is ``false`` in production and the monthly
invoice-close loop (``services.invoice_closer``) has therefore never run a
single tick. Before arming it, an operator needs to see what one sweep would do
PER TENANT — and the one thing they must not spend to find out is an invoice
number.

Numbers come from ``billing_invoice_counters``, a gapless per-(series, year)
row deliberately chosen over a Postgres ``SEQUENCE`` precisely because a
rollback must take the increment with it (see
``models.db.invoices.BillingInvoiceCounter``). A number that has been handed out
and committed is audit evidence; no later fix can un-issue it without either
renumbering statements already sent or minting a document for a hole in the
series. So a preview that consumes one is not a preview, it is an incident.

WHAT THIS PROVES
----------------
* Which tenants the sweep would classify as created / already-closed / refused /
  failed, for the month it would actually pick.
* That the refusal gates (bucket gaps, unpriced service keys, unpriced provider
  cost, seat count, byo_mode, licences reachability) genuinely fire or genuinely
  pass against today's data — because the real ``close_period`` runs, unmodified.
* That every INSERT the close performs satisfies the live schema: the SQL is
  really sent to Postgres (``flush``), so NOT NULL, the ``Numeric(20, 8)``
  scales, ``ux_billing_invoices_open_period``, ``ux_billing_invoices_number``,
  the status CHECK, the draft->issued immutability trigger and the counter's
  ``SELECT ... FOR UPDATE`` are all exercised for real.
* That nothing became durable — see THE DRY-RUN PROOF below.

WHAT THIS CANNOT PROVE
----------------------
* ⛔ **Leader election.** ``close_due_periods`` does no leader election of its
  own; ``services.invoice_closer`` wraps it in ``INVOICE_LOCK_KEY`` over the
  SESSION-mode pooler. This script calls the sweep directly, so a double-mint
  caused by two replicas both believing themselves leader is outside what it
  tests.
* ⛔ **The audit rows.** ``invoice_closer`` writes the ``audit_log`` row in a
  SECOND transaction, AFTER ``close_due_periods`` returns. That code is not
  called here at all, so an audit-write defect is invisible to this preview.
* ⛔ **Timing / scheduling.** Whether the loop fires on the right day, how it
  behaves across ``MAX_CONSECUTIVE_INVOICE_CLOSE_FAILURES``, and what it
  publishes to Prometheus are all ``invoice_closer``'s, not this script's.
* ⛔ **That the real run will produce these same figures.** The basis moves.
  Rollups get restated, licences answer differently, a markup rule opens. This
  is a measurement of NOW, not a forecast.
* ⚠️ **Anything about the scratch DB's data that is not also true of prod.** It
  refuses to touch production by construction (see SAFETY), so the tenant set,
  the rollups and therefore every figure below are the scratch database's.

THE DRY-RUN PROOF — mandatory, not decoration
---------------------------------------------
``close_due_periods`` commits internally, once per tenant, so it cannot be
wrapped in one outer transaction. Instead it is handed a session factory whose
sessions' ``commit()`` calls ``flush()`` — the SQL goes to Postgres, nothing
becomes durable, and every session is rolled back and closed in a ``finally``.

That is the mechanism, not the proof. The proof is that this script reads
``billing_invoice_counters`` (every row) and ``billing_invoices`` (count, and
count per status) on a SEPARATE committing connection both BEFORE and AFTER the
sweep, prints both, and exits non-zero with a loud banner if either moved. A
preview that silently burned a number is worse than no preview at all, so the
script is built to be able to accuse itself.

SAFETY
------
Three independent rails, all of which must pass:

1. ``--i-know-this-is-a-scratch-db`` must be passed explicitly.
2. The DSN host must not be a production Postgres host (a private-service
   hostname, a Kubernetes in-cluster service name, or any raw IP address).
   There is no default DSN.
3. ``ENVIRONMENT`` must be set and must not be ``production``.

USAGE
-----
::

    ENVIRONMENT=development \\
    # (the DSN is credential-shaped, so it is described rather than spelled out:
    #  scheme postgresql+psycopg, your own scratch host/port, a _scratch database)
    PREVIEW_DATABASE_URL="$MY_SCRATCH_DSN" \\
        python scripts/preview_invoice_close.py --i-know-this-is-a-scratch-db

    # preview a chosen month (the sweep closes the month BEFORE --now):
    ... --i-know-this-is-a-scratch-db --now 2026-08-15T00:00:00+00:00

Log lines from the service code go to stderr; the report goes to stdout, so
``2>/dev/null`` gives a clean report.

EXIT CODES
----------
``0`` preview ran and is PROVEN dry · ``1`` THE PROOF FAILED (the database
changed) · ``2`` refused to run (a safety rail) · ``3`` the sweep itself raised.
"""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

import structlog  # noqa: E402
from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from models.db import BillingInvoice, BillingInvoiceCounter  # noqa: E402

# ⭐ `_MONEY_COLUMNS` and `_previous_month` are imported rather than copied, and
# the leading underscore is accepted on purpose. `_MONEY_COLUMNS` is the set
# `close_period` derives, hashes into `snapshot_sha256` and `reproduce`
# re-verifies — its own comment says it is "named once" so the hashed set and
# the verified set cannot drift into being two different sets. A preview that
# re-listed the money columns by hand would become a THIRD list, and would go on
# printing a confident table with a column silently missing the first time one is
# added. Same argument for `_previous_month`: the month this script PRINTS and
# the month the sweep CLOSES must be the same computation, or the operator has
# been told about a month that was never previewed.
from services.invoices import (  # noqa: E402
    _MONEY_COLUMNS,
    _previous_month,
    close_due_periods,
)
from services.tenant_names import get_tenant_names  # noqa: E402
from settings import Settings  # noqa: E402

# --------------------------------------------------------------------------- #
# Safety rails
# --------------------------------------------------------------------------- #

#: Host shapes that are production Postgres, or a pooler in front of it: any
#: hostname under the private-service TLD, any Kubernetes in-cluster service
#: name, or ANY raw IP literal. ⛔ MATCHED ON THE HOST, NOT host:port, and that
#: is deliberately STRICTER than a host:port match. There is no non-production
#: database on any such host, so a port that happens to differ is not evidence
#: of a scratch target — it is far more likely a tunnel, a second pooler, or a
#: typo. The cost of being too strict is a refused preview and a one-line
#: argument; the cost of being too loose is a burned invoice number that cannot
#: be given back. Fail toward the refusal.
_INTERNAL_TLD_LABEL = "internal"
_IN_CLUSTER_DOMAIN_LABELS = ("svc", "cluster", "local")

#: The structlog events `close_due_periods` emits that carry a `tenant_id`.
#: ⚠️ `CloseDueOutcome` returns `refused` and `failed` as bare COUNTS, so these
#: log events are the ONLY per-tenant detail the sweep exposes without editing
#: `invoices.py`. See `_CaptureProcessor` for why capturing them beats
#: reimplementing the sweep.
_EVENT_UNCHANGED = "invoice_close_due_unchanged"
#: ⚠️ ADDED 2026-09-13. Without it the report detailed 7 of 35 candidates and said
#: so — honest, but an operator reading "WOULD_SKIP=1" beside a census of
#: "skipped=28" has to reconcile two numbers that disagree. A skip is the
#: MAJORITY outcome of a real sweep, so it is the one class that must not be
#: missing from the detail.
_EVENT_SKIPPED = "invoice_close_due_skipped"
_EVENT_REFUSED = "invoice_close_due_refused"
_EVENT_FAILED = "invoice_close_due_failed"
_EVENT_COMPLETE = "invoice_close_due_complete"
_WATCHED_EVENTS = frozenset(
    {_EVENT_UNCHANGED, _EVENT_SKIPPED, _EVENT_REFUSED, _EVENT_FAILED, _EVENT_COMPLETE}
)

CLASS_CREATE = "WOULD_CREATE"
CLASS_SKIP = "WOULD_SKIP"
#: ⛔ ITS OWN CLASS, NOT FOLDED INTO WOULD_SKIP. A period that already holds an
#: invoice and a period that charges nothing are different answers to "why will
#: this tenant not be invoiced": the first is idempotence and the second is
#: policy. Merging them hides whether a sweep is re-observing old work or
#: declining new work.
CLASS_UNCHANGED = "WOULD_LEAVE_ALONE"
CLASS_REFUSE = "WOULD_REFUSE"
CLASS_FAIL = "WOULD_FAIL"
_CLASS_ORDER = (CLASS_CREATE, CLASS_SKIP, CLASS_UNCHANGED, CLASS_REFUSE, CLASS_FAIL)


# --------------------------------------------------------------------------- #
# The non-committing session
# --------------------------------------------------------------------------- #


@dataclass
class _PreviewedInvoice:
    """A snapshot of one invoice taken while its session was still LIVE.

    ⛔ THE SNAPSHOT IS NOT AN OPTIMISATION, it is the only way to read these
    figures at all. `CloseDueOutcome.created` holds ORM objects whose sessions
    this script rolls back and closes in its `finally`; `Session.rollback()`
    EXPIRES every instance it still manages, and an expired + detached instance
    raises `DetachedInstanceError` on the first attribute access. So the money
    is copied out into plain `str`/`int` at the one moment it is cheap and safe —
    inside the intercepted `commit()`, after the flush that populated it.

    `Decimal` is carried as `str`, never `float`: a float round-trip is a silent
    edit to a money figure (same rule as `invoices._jsonable`).
    """

    tenant_id: str
    tenant_name: str
    invoice_number: str | None
    status: str
    billing_mode: str
    markup_pct: str
    markup_source: str
    excluded_unbillable_rows: int
    money: dict[str, str | int | None]


def _snapshot_invoices(session: AsyncSession) -> list[_PreviewedInvoice]:
    """Copy every live ``BillingInvoice`` out of ``session`` as plain values.

    Reads through ``sync_session`` rather than ``AsyncSession``'s proxied view:
    the identity map is a sync-side structure and going at it directly cannot be
    affected by which attributes the async proxy re-exports across SQLAlchemy
    versions. (Measured on 2.0.51: ``sync_session`` is an INSTANCE attribute, not
    a class-level descriptor — ``hasattr(AsyncSession, 'sync_session')`` is
    ``False`` — so this must stay a runtime access on ``self``.)

    Called AFTER the flush, so the rows are persistent in this (still open)
    session and every attribute ``close_period`` set — including the
    ``invoice_number`` ``issue()`` just minted — is loaded. ``new`` is unioned in
    as braces: it is empty after a flush, but a future call path that commits
    without flushing first would otherwise snapshot nothing and report a silent
    zero rather than a figure.

    ⚠️ ``identity_map`` is a ``WeakInstanceDict``, so a clean persistent object
    with no other referrer can vanish from it. It does not here — the invoice is
    held on ``close_due_periods``' own stack frame across the ``commit()`` call —
    but that is WHY this is safe, not an accident to rely on silently. Snapshotted
    through ``list(...)`` so the set cannot change under iteration.
    """
    snapshots: list[_PreviewedInvoice] = []
    sync = session.sync_session
    seen: set[int] = set()
    for obj in [*sync.identity_map.values(), *sync.new]:
        if not isinstance(obj, BillingInvoice) or id(obj) in seen:
            continue
        seen.add(id(obj))
        snapshots.append(
            _PreviewedInvoice(
                tenant_id=str(obj.tenant_id),
                tenant_name=obj.tenant_name or "",
                invoice_number=obj.invoice_number,
                status=obj.status,
                billing_mode=obj.billing_mode,
                markup_pct=_money_str(obj.markup_pct),
                markup_source=obj.markup_source,
                excluded_unbillable_rows=int(obj.excluded_unbillable_rows or 0),
                money={col: _money_str(getattr(obj, col, None)) for col in _MONEY_COLUMNS},
            )
        )
    return snapshots


def _money_str(value: Any) -> Any:
    """``Decimal`` -> its exact text; everything else unchanged."""
    return str(value) if isinstance(value, Decimal) else value


def _make_non_committing_session_class(sink: list[_PreviewedInvoice]) -> type[AsyncSession]:
    """An ``AsyncSession`` subclass whose ``commit()`` flushes instead.

    ⭐ A SUBCLASS, NOT A MONKEYPATCHED INSTANCE ATTRIBUTE — and the reason is
    measured, not assumed. On the pinned SQLAlchemy 2.0.51, ``AsyncSession``
    DOES declare ``__slots__ = ('__weakref__',)``; instances happen to still
    carry a ``__dict__`` because a base class does not declare slots, so
    ``session.commit = ...`` would in fact work TODAY. That is exactly the
    problem: it works by inheritance accident, on a library we pin but do not
    own, and the failure mode if that accident ever changes — a slotted base, or
    ``commit`` being re-exported through the sync-session proxy — is that the
    assignment is silently dropped, the commit REALLY commits, and the preview
    burns an invoice number. A mechanism whose failure mode is "the safety was
    off and nothing said so" must not be reachable by a dependency bump. An
    override is resolved by ordinary MRO and cannot be bypassed.

    ``flush()`` is what makes the dry run honest rather than theatrical: the
    INSERT/UPDATE statements are genuinely sent, so every constraint, trigger and
    the counter's ``FOR UPDATE`` really run. Durability — and only durability — is
    withheld. The enclosing transaction is then discarded by
    ``_DryRunSessionFactory.discard_all``, and by the ``async with`` inside
    ``close_due_periods`` before that.

    The class is built in a closure so the snapshot sink is captured per run,
    rather than smuggled through a module global that a second run would share.
    """

    class _NonCommittingSession(AsyncSession):
        async def commit(self) -> None:  # type: ignore[override]
            await self.flush()
            # Taken HERE because this is the last instant the invoice is both
            # fully populated and attached to a live session. See
            # `_PreviewedInvoice`.
            sink.extend(_snapshot_invoices(self))

    return _NonCommittingSession


class _DryRunSessionFactory:
    """Hands out non-committing sessions and remembers every one of them.

    ``close_due_periods`` takes an ``async_sessionmaker`` and only ever calls it
    and uses the result as an async context manager, so a plain callable is
    substitutable at runtime; the ``cast`` at the call site is for the type
    checker, not a claim about the shape.

    The register exists for the ``finally``. ``close_due_periods``' own
    ``async with`` already closes each session on the happy path AND on a
    per-tenant exception, so this is belt-and-braces — but the thing being
    braced against is an uncommitted transaction holding the gapless counter's
    ``FOR UPDATE`` lock, which would then block this script's own "after" probe
    forever and turn a clean refusal into a hang.
    """

    def __init__(self, inner: async_sessionmaker[AsyncSession]) -> None:
        self._inner = inner
        self._handed_out: list[AsyncSession] = []

    def __call__(self, **kwargs: Any) -> AsyncSession:
        session = self._inner(**kwargs)
        self._handed_out.append(session)
        return session

    @property
    def handed_out(self) -> int:
        return len(self._handed_out)

    async def discard_all(self) -> list[str]:
        """Roll back and close every session handed out. Never raises."""
        problems: list[str] = []
        for session in self._handed_out:
            try:
                await session.rollback()
            except Exception as exc:  # pragma: no cover - teardown must not mask
                problems.append(f"rollback: {type(exc).__name__}: {exc}")
            try:
                await session.close()
            except Exception as exc:  # pragma: no cover - teardown must not mask
                problems.append(f"close: {type(exc).__name__}: {exc}")
        return problems


# --------------------------------------------------------------------------- #
# Per-tenant detail, via structlog capture
# --------------------------------------------------------------------------- #


@dataclass
class _CapturedEvent:
    event: str
    fields: dict[str, Any]
    exc_type: str | None
    exc_message: str | None


def _install_log_capture(sink: list[_CapturedEvent]) -> None:
    """Configure structlog so this script can read the sweep's own events.

    ⭐ WHY CAPTURE THE LOG RATHER THAN RE-DRIVE THE SWEEP. `CloseDueOutcome`
    returns `refused` and `failed` as integers, so the per-tenant detail the
    operator needs exists ONLY in `invoice_close_due_refused` /
    `..._failed` / `..._unchanged`. The alternatives were:

      (a) edit `invoices.py` to return per-tenant detail — out of scope, and this
          script exists precisely to observe the code AS DEPLOYED;
      (b) reimplement the sweep here (collect tenant ids, loop, call
          `close_period`, catch per tenant) — which would make the preview a
          SECOND implementation of the thing under test. Its classification could
          then diverge from the real loop's in exactly the cases that matter most
          (a new gate, a re-parented exception, a changed `except` arm), and it
          would do so while still printing a confident table. A preview that can
          disagree with production about what production would do is worse than
          no preview;
      (c) capture the structlog events the real sweep already emits. Chosen. It
          needs no change to `invoices.py`, and it classifies using the sweep's
          OWN `except` arms, so it cannot drift from them.

    The capture processor is FIRST in the chain, ahead of
    ``structlog.stdlib.filter_by_level``, on purpose: ``..._unchanged`` is logged
    at INFO and would otherwise be dropped before it reached us whenever the root
    level is WARNING, and a silently missing class of rows is the defect this
    whole script is written against.

    The exception is read from ``exc_info`` (and, for the ``exc_info=True`` form
    the sweep actually uses, from ``sys.exc_info()``) — the processor chain runs
    synchronously inside the ``except`` block, so the live exception is still
    there. It is read BEFORE ``format_exc_info`` replaces it with a rendered
    traceback string, which is why this processor has to be first rather than
    merely early.

    ``cache_logger_on_first_use=False`` because ``services.invoices`` binds its
    logger at import time; with caching on, anything that configured structlog
    before this call (``logging_config.configure_logging``, imported transitively)
    would have pinned the old chain and this capture would silently record
    nothing.
    """

    def _capture(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        name = event_dict.get("event")
        if name in _WATCHED_EVENTS:
            exc_type: str | None = None
            exc_message: str | None = None
            raw = event_dict.get("exc_info")
            typ: type[BaseException] | None = None
            val: BaseException | None = None
            if isinstance(raw, BaseException):
                typ, val = type(raw), raw
            elif isinstance(raw, tuple) and len(raw) == 3:
                typ, val = raw[0], raw[1]
            elif raw:
                typ, val, _tb = sys.exc_info()
            if typ is not None:
                exc_type = typ.__name__
                exc_message = str(val) if val is not None else ""
            sink.append(
                _CapturedEvent(
                    event=str(name),
                    fields={k: v for k, v in event_dict.items() if k != "exc_info"},
                    exc_type=exc_type,
                    exc_message=exc_message,
                )
            )
        return event_dict

    structlog.configure(
        processors=[
            _capture,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
    # The service's log lines go to STDERR so the report on stdout stays
    # machine-readable-ish and `2>/dev/null` yields a clean table.
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=logging.INFO, force=True)


# --------------------------------------------------------------------------- #
# The proof: ledger state before and after
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _LedgerState:
    """The committed facts a dry run must leave untouched."""

    #: ``{scope: (period_year, next_seq)}`` for EVERY counter row. The whole
    #: table, not just the scope we expect to be drawn from: a preview that
    #: somehow advanced a DIFFERENT series must be caught too.
    counters: dict[str, tuple[int, int]]
    invoice_count: int
    #: Per-status counts. ⭐ Belt to `invoice_count`'s braces: a net-zero pair of
    #: mutations (one row inserted, one voided) leaves the total identical, and
    #: a proof that can be satisfied by two wrongs is not a proof.
    by_status: dict[str, int]

    def differs_from(self, other: _LedgerState) -> list[str]:
        diffs: list[str] = []
        if self.invoice_count != other.invoice_count:
            diffs.append(f"billing_invoices count {self.invoice_count} -> {other.invoice_count}")
        if self.by_status != other.by_status:
            diffs.append(f"billing_invoices by status {self.by_status} -> {other.by_status}")
        for scope in sorted(set(self.counters) | set(other.counters)):
            before = self.counters.get(scope)
            after = other.counters.get(scope)
            if before != after:
                diffs.append(f"billing_invoice_counters[{scope}] {before} -> {after}")
        return diffs


async def _read_ledger_state(factory: async_sessionmaker[AsyncSession]) -> _LedgerState:
    """Read the ledger on a NORMAL session, in its own fresh transaction.

    ⚠️ Deliberately NOT the dry-run factory: a session whose `commit()` does not
    commit is also a session whose transaction can see its own uncommitted
    writes, so reading the "after" state through it would report the very rows
    that are about to be discarded and the proof would accuse a clean run. A
    separate session starting a new READ COMMITTED transaction sees exactly what
    is durable, which is the only thing this check is about.

    ⚠️ Must be called only when no dry-run session is still open. An abandoned
    transaction holding the counter's ``FOR UPDATE`` would block this read.
    """
    async with factory() as session:
        counters = {
            row.scope: (int(row.period_year), int(row.next_seq))
            for row in (await session.execute(select(BillingInvoiceCounter))).scalars()
        }
        invoice_count = int(
            (await session.execute(select(func.count()).select_from(BillingInvoice))).scalar_one()
        )
        by_status = {
            str(status): int(n)
            for status, n in (
                await session.execute(
                    select(BillingInvoice.status, func.count()).group_by(BillingInvoice.status)
                )
            ).all()
        }
        # Read-only: end the transaction explicitly rather than leaving it to
        # close(), so nothing idle-in-transaction outlives this call.
        await session.rollback()
    return _LedgerState(counters=counters, invoice_count=invoice_count, by_status=by_status)


# --------------------------------------------------------------------------- #
# Rails
# --------------------------------------------------------------------------- #


class _RefusedError(Exception):
    """A safety rail said no. Carries the operator-facing reason."""


def _resolve_dsn(args: argparse.Namespace) -> str:
    """The target DSN, from ``--dsn`` / ``PREVIEW_DATABASE_URL`` / ``DATABASE_URL``.

    ⛔ NO DEFAULT. A hardcoded fallback is how a script ends up pointed at prod
    by an operator who simply forgot the argument, and the whole point of this
    file is that one such mistake is unrecoverable.
    """
    dsn = args.dsn or os.environ.get("PREVIEW_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise _RefusedError(
            "no DSN: pass --dsn, or set PREVIEW_DATABASE_URL (preferred) or DATABASE_URL. "
            "There is no default — a default is how this script would end up on prod."
        )
    return dsn


def _forbidden_dsn_host(dsn: str) -> str | None:
    """Return the offending host in ``dsn``, or ``None`` when none is present.

    A raw IP literal is refused even when it is not in a known range: there is
    no non-production database on a production host, and an operator who pasted
    the address instead of the name has not made this any less production.
    Names that are not IP literals are matched by DNS label shape.
    """
    host = (urlsplit(dsn).hostname or "").strip(".")
    if not host:
        return None
    labels = host.split(".")
    if labels[-1] == _INTERNAL_TLD_LABEL or tuple(labels[-3:]) == _IN_CLUSTER_DOMAIN_LABELS:
        return host
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return None
    return host


def _check_rails(args: argparse.Namespace, dsn: str) -> None:
    """Three independent rails. All must pass; each names what to do."""
    if not args.i_know_this_is_a_scratch_db:
        raise _RefusedError(
            "refusing to run without --i-know-this-is-a-scratch-db. This runs the REAL "
            "close against the DSN you give it; the flag is the operator stating that "
            "the target is disposable."
        )

    # Host check. `Settings.model_post_init` rewrites only the SCHEME
    # (postgresql:// -> postgresql+psycopg://), never the host, so parsing the
    # hostname once covers both spellings.
    offender = _forbidden_dsn_host(dsn)
    if offender is not None:
        raise _RefusedError(
            f"DSN names {offender!r}, which is production Postgres (or a pooler in front "
            "of it). This script mints real invoice numbers on whatever it is pointed "
            "at; a burned number in the gapless series cannot be given back. Point it "
            "at a scratch database."
        )

    # ⚠️ Read the ENV VAR, not `settings.environment`, and read it BEFORE
    # constructing Settings. `Settings.environment` DEFAULTS TO "production", and
    # `_enforce_production_required_fields` then raises a ValidationError for the
    # missing production-only fields — so a Settings-first check would blow up
    # with a confusing pydantic traceback instead of this rail's actual message.
    # Unset therefore reads as production and is REFUSED, which is the direction
    # that fails safe.
    environment = os.environ.get("ENVIRONMENT")
    if not environment:
        raise _RefusedError(
            "ENVIRONMENT is unset, and Settings.environment DEFAULTS to 'production'. "
            "Set ENVIRONMENT=development (or your scratch tag) explicitly."
        )
    if environment.strip().lower() == "production":
        raise _RefusedError(f"ENVIRONMENT={environment!r}. This script does not run in production.")


def _resolve_now(raw: str | None) -> tuple[datetime, list[str]]:
    """Parse ``--now``, returning it and any warnings the operator must see."""
    warnings: list[str] = []
    if raw is None:
        return datetime.now(UTC), warnings
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise _RefusedError(f"--now {raw!r} is not ISO-8601: {exc}") from exc
    if parsed.tzinfo is None:
        # Not silently assumed: the period is derived from `now.year`/`now.month`,
        # so near a month boundary the assumed zone decides WHICH MONTH is
        # previewed. The operator is told.
        parsed = parsed.replace(tzinfo=UTC)
        warnings.append(f"--now {raw!r} has no timezone; read as UTC ({parsed.isoformat()})")
    return parsed, warnings


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #


@dataclass
class _Row:
    tenant_id: str
    klass: str
    name: str = ""
    detail: str = ""
    invoice: _PreviewedInvoice | None = None


@dataclass
class _Discrepancies:
    """Where the report knows it is incomplete. Printed, never swallowed."""

    notes: list[str] = field(default_factory=list)


def _build_rows(
    outcome_created: list[_PreviewedInvoice],
    captured: list[_CapturedEvent],
    names: dict[str, str],
) -> tuple[list[_Row], _Discrepancies]:
    """Classify every tenant the sweep touched.

    Creates come from the snapshots (they carry the money). Skips, refusals and
    failures come from the sweep's own log events — which is also what makes the
    classification the sweep's rather than this script's.
    """
    disc = _Discrepancies()
    rows: dict[str, _Row] = {}

    for snap in outcome_created:
        rows[snap.tenant_id] = _Row(
            tenant_id=snap.tenant_id,
            klass=CLASS_CREATE,
            name=snap.tenant_name or names.get(snap.tenant_id.lower(), ""),
            invoice=snap,
        )

    for ev in captured:
        tenant_id = ev.fields.get("tenant_id")
        if tenant_id is None:
            continue
        tenant_id = str(tenant_id)
        name = names.get(tenant_id.lower(), "")
        if ev.event == _EVENT_UNCHANGED:
            rows.setdefault(
                tenant_id,
                _Row(
                    tenant_id=tenant_id,
                    klass=CLASS_UNCHANGED,
                    name=name,
                    detail=(
                        f"already has a live invoice "
                        f"{ev.fields.get('invoice_id')} ({ev.fields.get('status')})"
                    ),
                ),
            )
        elif ev.event == _EVENT_SKIPPED:
            rows.setdefault(
                tenant_id,
                _Row(
                    tenant_id=tenant_id,
                    klass=CLASS_SKIP,
                    name=name,
                    detail="charges nothing — no invoice number drawn",
                ),
            )
        elif ev.event in (_EVENT_REFUSED, _EVENT_FAILED):
            klass = CLASS_REFUSE if ev.event == _EVENT_REFUSED else CLASS_FAIL
            # `refusal` is present only on the named-upstream arm; the bare
            # `except InvoiceError` arm carries the class in exc_info only. Both
            # are read, in that order, so neither arm reports "unknown".
            exc_type = ev.exc_type or ev.fields.get("refusal") or "unknown"
            detail = f"{exc_type}: {ev.exc_message or '(no message captured)'}"
            rows[tenant_id] = _Row(tenant_id=tenant_id, klass=klass, name=name, detail=detail)

    # ⭐ CROSS-CHECK THE INSTRUMENT AGAINST THE SWEEP'S OWN TALLY. If the log
    # capture were blinded — a renamed event, a reconfigured structlog, a
    # processor ordering change — the table would simply be short some rows and
    # would still look authoritative. `invoice_close_due_complete` carries the
    # sweep's own counts, so the two can be compared, and a mismatch is printed
    # as a defect in THIS SCRIPT rather than as a finding about the tenants.
    complete = next((e for e in captured if e.event == _EVENT_COMPLETE), None)
    if complete is None:
        disc.notes.append(
            f"{_EVENT_COMPLETE!r} was never captured — either the sweep did not finish or "
            "the log capture is blind. Treat the table below as INCOMPLETE."
        )
    else:
        # ⚠️ ONE KEY PER CLASS, AND THEY ARE NOT INTERCHANGEABLE. This map used to
        # send CLASS_SKIP to the sweep's `unchanged` count, which was the same
        # conflation the detail classifier made: it reconciled cleanly while 28 of
        # 35 candidates went undetailed, because the number it checked was the one
        # it could already satisfy. A reconciliation that cannot fail is not one.
        expected = {
            CLASS_CREATE: int(complete.fields.get("created") or 0),
            CLASS_SKIP: int(complete.fields.get("skipped") or 0),
            CLASS_UNCHANGED: int(complete.fields.get("unchanged") or 0),
            CLASS_REFUSE: int(complete.fields.get("refused") or 0),
            CLASS_FAIL: int(complete.fields.get("failed") or 0),
        }
        seen = {k: sum(1 for r in rows.values() if r.klass == k) for k in _CLASS_ORDER}
        for klass in _CLASS_ORDER:
            if seen[klass] != expected[klass]:
                disc.notes.append(
                    f"{klass}: the sweep reported {expected[klass]} but this script could only "
                    f"detail {seen[klass]}. The table is INCOMPLETE for this class."
                )
        candidates = int(complete.fields.get("candidates") or 0)
        if candidates != len(rows):
            disc.notes.append(
                f"the sweep saw {candidates} candidate tenants; this script detailed {len(rows)}."
            )

    return sorted(rows.values(), key=lambda r: (_CLASS_ORDER.index(r.klass), r.tenant_id)), disc


def _print_report(
    *,
    month: str,
    period: tuple[Any, Any],
    now: datetime,
    dsn_redacted: str,
    rows: list[_Row],
    disc: _Discrepancies,
    warnings: list[str],
) -> None:
    out = sys.stdout.write
    out("\n")
    out("=" * 100 + "\n")
    out("INVOICE CLOSE — DRY RUN PREVIEW (nothing below was committed)\n")
    out("=" * 100 + "\n")
    for w in warnings:
        out(f"⚠️  {w}\n")
    out(f"target DSN      : {dsn_redacted}\n")
    out(f"--now resolved  : {now.isoformat()}\n")
    # ⚠️ Spelled out in full because the sweep closes the PREVIOUS month. An
    # operator who reads `--now` as the month under preview has learned nothing.
    out(
        f"PREVIEWED MONTH : {month}   period [{period[0].isoformat()}, "
        f"{period[1].isoformat()})  — the month BEFORE --now, end EXCLUSIVE\n"
    )
    out("-" * 100 + "\n")

    if not rows:
        out("no tenant had activity in this period — nothing would be closed\n")
    else:
        out(f"{'CLASS':<13} {'TENANT':<38} {'NAME':<22} DETAIL\n")
        for row in rows:
            if row.invoice is not None:
                money = row.invoice.money
                detail = (
                    f"total={_display(money.get('total_usd'))} "
                    f"access={_display(money.get('access_charge_usd'))} "
                    f"seats={money.get('seat_count')!r} "
                    f"llm={_display(money.get('llm_usage_usd'))}"
                    f"+markup {_display(money.get('llm_markup_usd'))} "
                    f"no={row.invoice.invoice_number}"
                )
            else:
                detail = row.detail
            out(f"{row.klass:<13} {row.tenant_id:<38} {row.name[:22]:<22} {detail}\n")

        creates = [r for r in rows if r.invoice is not None]
        if creates:
            out("\n")
            out("-" * 100 + "\n")
            out("MONEY LINES, EXACT STORED VALUES (the table above is display-rounded)\n")
            out("-" * 100 + "\n")
            for row in creates:
                inv = row.invoice
                assert inv is not None  # narrowed by the filter above
                out(f"\n{row.tenant_id}  {row.name or '(name unresolved)'}\n")
                out(
                    f"  would draw invoice_number={inv.invoice_number!r} "
                    f"status={inv.status!r} billing_mode={inv.billing_mode!r}\n"
                )
                out(
                    f"  markup_pct={inv.markup_pct} source={inv.markup_source!r} "
                    f"excluded_rows={inv.excluded_unbillable_rows}\n"
                )
                for col in _MONEY_COLUMNS:
                    out(f"    {col:<26} {inv.money.get(col)}\n")

    out("\n")
    out("-" * 100 + "\n")
    counts = {k: sum(1 for r in rows if r.klass == k) for k in _CLASS_ORDER}
    out(f"SUMMARY for {month}: " + "  ".join(f"{k}={counts[k]}" for k in _CLASS_ORDER) + "\n")
    out(f"candidate tenants detailed: {len(rows)}\n")

    if disc.notes:
        out("\n")
        out("!" * 100 + "\n")
        out("THIS REPORT KNOWS IT IS INCOMPLETE:\n")
        for note in disc.notes:
            out(f"  - {note}\n")
        out("!" * 100 + "\n")


def _display(value: Any) -> str:
    """2-decimal rendering FOR THE SUMMARY LINE ONLY.

    The exact stored value is always also printed, in the money-lines block. A
    rounded figure is readable; a rounded figure presented as the only figure is
    a money report that lies by a cent.
    """
    if value is None:
        return "-"
    try:
        return f"{Decimal(str(value)):.2f}"
    except Exception:
        return str(value)


def _redact(dsn: str) -> str:
    """Hide the password so the report can be pasted into a ticket."""
    if "://" not in dsn:
        return dsn
    scheme, rest = dsn.split("://", 1)
    if "@" not in rest:
        return dsn
    creds, host = rest.rsplit("@", 1)
    user = creds.split(":", 1)[0]
    return f"{scheme}://{user}:***@{host}"


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


async def _run(args: argparse.Namespace) -> int:
    dsn = _resolve_dsn(args)
    _check_rails(args, dsn)
    now, warnings = _resolve_now(args.now)

    # Settings comes from the environment (so markup defaults, service URLs and
    # every gate's configuration are the real ones) with ONLY the DSN overridden,
    # so the preview cannot read one database and be configured for another.
    settings = Settings(database_url=dsn)
    # ⚠️ Re-check the NORMALISED DSN: model_post_init rewrites the scheme, and the
    # rail must hold on whatever string actually reaches the engine.
    _check_rails(args, settings.database_url)

    period_start, period_end = _previous_month(now)
    month = f"{period_start.year:04d}-{period_start.month:02d}"

    captured: list[_CapturedEvent] = []
    _install_log_capture(captured)

    snapshots: list[_PreviewedInvoice] = []
    engine: AsyncEngine = create_async_engine(settings.database_url)
    probe_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    dry_factory = _DryRunSessionFactory(
        async_sessionmaker(
            bind=engine,
            class_=_make_non_committing_session_class(snapshots),
            expire_on_commit=False,
        )
    )

    sweep_error: BaseException | None = None
    teardown_problems: list[str] = []
    try:
        before = await _read_ledger_state(probe_factory)
        try:
            await close_due_periods(
                cast("async_sessionmaker[AsyncSession]", dry_factory),
                settings,
                now=now,
            )
        except Exception as exc:
            # The sweep catches per tenant, so reaching here means something
            # outside the per-tenant loop broke. The PROOF still has to be
            # printed — a run that died halfway is exactly when "did it leave a
            # number behind?" most needs answering.
            sweep_error = exc
        finally:
            teardown_problems = await dry_factory.discard_all()

        # Only now that every dry-run transaction is gone: an open one would hold
        # the counter's FOR UPDATE and block this read.
        after = await _read_ledger_state(probe_factory)

        # Names are a convenience, and an outage of the registry must not cost
        # the operator the report. `get_tenant_names` already returns {} on any
        # failure, and this guard covers the rest.
        names: dict[str, str] = {}
        try:
            names = await get_tenant_names(settings)
        except Exception as exc:
            warnings.append(f"tenant names unavailable ({type(exc).__name__}); showing uuids only")

        rows, disc = _build_rows(snapshots, captured, names)
        if teardown_problems:
            disc.notes.append("session teardown reported: " + "; ".join(teardown_problems))
        _print_report(
            month=month,
            period=(period_start, period_end),
            now=now,
            dsn_redacted=_redact(settings.database_url),
            rows=rows,
            disc=disc,
            warnings=warnings,
        )

        exit_code = _print_proof(before, after)
        if sweep_error is not None:
            sys.stdout.write(
                f"\n⛔ THE SWEEP ITSELF RAISED: {type(sweep_error).__name__}: {sweep_error}\n"
                "   The table above covers only the tenants it reached.\n"
            )
            # A failed PROOF outranks a failed sweep: a burned number is the
            # worse fact and must own the exit code.
            return 1 if exit_code == 1 else 3
        return exit_code
    finally:
        await engine.dispose()


def _print_proof(before: _LedgerState, after: _LedgerState) -> int:
    """Print the before/after ledger state. Return 0 if unchanged, else 1."""
    out = sys.stdout.write
    out("\n")
    out("=" * 100 + "\n")
    out("PROOF THE DRY RUN WAS DRY\n")
    out("=" * 100 + "\n")
    n_before, n_after = before.invoice_count, after.invoice_count
    out(f"  billing_invoices count      before={n_before}  after={n_after}\n")
    out(f"  billing_invoices by status  before={before.by_status}  after={after.by_status}\n")
    out("  billing_invoice_counters:\n")
    scopes = sorted(set(before.counters) | set(after.counters))
    if not scopes:
        out("    (no counter rows — the series has never been drawn from)\n")
    for scope in scopes:
        b = before.counters.get(scope)
        a = after.counters.get(scope)
        mark = "  OK" if b == a else "  <<< CHANGED"
        out(f"    {scope:<24} before={b}  after={a}{mark}\n")

    diffs = before.differs_from(after)
    if not diffs:
        out("\n✅ UNCHANGED — no invoice row was written and no invoice number was consumed.\n")
        return 0

    out("\n")
    out("#" * 100 + "\n")
    out("#  FAILURE: THE PREVIEW MUTATED THE DATABASE. IT WAS NOT A DRY RUN.\n")
    out("#\n")
    for d in diffs:
        out(f"#    {d}\n")
    out("#\n")
    out("#  If a counter advanced, a number from the GAPLESS series was consumed and the\n")
    out("#  series now has a hole that cannot be repaired after the fact. Treat this as an\n")
    out("#  incident on the target database and do NOT re-run until the cause is found.\n")
    out("#" * 100 + "\n")
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run the monthly invoice close: run the real close logic against a "
            "SCRATCH database, report what it would do per tenant, commit nothing, and "
            "prove nothing was committed."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--i-know-this-is-a-scratch-db",
        action="store_true",
        help=(
            "REQUIRED. States that the target database is disposable. Without it the "
            "script refuses to run."
        ),
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help=(
            "Target DSN. Defaults to $PREVIEW_DATABASE_URL, then $DATABASE_URL. There is "
            "no built-in default, deliberately."
        ),
    )
    parser.add_argument(
        "--now",
        default=None,
        help=(
            "ISO-8601 instant to evaluate as 'now' (default: real now). The sweep closes "
            "the month BEFORE this; the resolved period is printed."
        ),
    )
    args = parser.parse_args()
    try:
        sys.exit(asyncio.run(_run(args)))
    except _RefusedError as exc:
        sys.stderr.write(f"\n⛔ REFUSED: {exc}\n\n")
        sys.exit(2)


if __name__ == "__main__":
    main()
