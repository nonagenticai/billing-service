"""Shared parser helpers used by both event_flusher and cost_flusher.

Internal to the services package — leading underscore signals not for
import outside services/.
"""

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from tenancy import (
    NIL_TENANT_ID,
    canonical_tenant_id,
    looks_like_credential,
    redact_tenant_id,
)


def _parse_uuid(val: Any) -> uuid.UUID | None:
    """Parse UUID from string, return None on failure."""
    if not val:
        return None
    try:
        return uuid.UUID(val)
    except (ValueError, AttributeError):  # fmt: skip
        return None


# The nil uuid is the single "we could not attribute this" tenant. It is a REAL
# registered tenant row (``licenses.tenants`` name ``default-anon``) and, since
# migration 0011 made every tenant_id column a Postgres ``uuid``, it is also the
# only sentinel that can still be written at all — the previous string sentinels
# ("__unattributed__", "unknown") now raise DataError at the INSERT.
#
# ``UNATTRIBUTED_TENANT_ID`` is kept as an alias for tests/test_cost_flusher.py,
# which imports it by that name. ``cost_flusher`` no longer needs it: since
# ``_cost_schema.validate_cost_event`` began rejecting an unattributable tenant
# (2026-08-26) the sentinel cannot reach a cost row at all, so the exclusion the
# flusher used to apply against it is gone. ``event_flusher`` (the USAGE stream)
# still sinks to it.
UNATTRIBUTED_TENANT_ID = NIL_TENANT_ID

# Re-exported so existing ``from services._parsers import ...`` call sites keep
# working; the single definition of each now lives in :mod:`tenancy`, shared with
# the HTTP boundary in auth_deps.
_looks_like_credential = looks_like_credential
_redact_tenant_id = redact_tenant_id


def _normalize_tenant_id(raw: Any) -> tuple[str, bool]:
    """Coerce an emitted tenant_id to a uuid string, returning ``(tenant_id, is_non_uuid)``.

    ALWAYS returns something a ``uuid`` column accepts. Since migration 0011 that
    is not a nicety: any other return value raises
    ``sqlalchemy.exc.DataError: invalid input syntax for type uuid`` inside the
    flusher's INSERT and takes the whole batch with it.

    Resolution:

    - a valid uuid with a non-zero int  -> its canonical form, ``False``
    - the nil uuid                      -> :data:`NIL_TENANT_ID`, ``True``
    - a leaked credential               -> :data:`NIL_TENANT_ID`, ``True``
    - missing / empty                   -> :data:`NIL_TENANT_ID`, ``True``
    - a known legacy tenant NAME        -> its registered uuid, ``False``
    - anything else                     -> :data:`NIL_TENANT_ID`, ``True``

    CHANGED: missing/empty used to return the literal ``"unknown"``. That MINTED A
    FAKE TENANT — 13,670 live rows carry it — and it can no longer be stored at
    all. It now sinks to the nil tenant like every other unattributable value.

    ``is_non_uuid`` still drives the observability log/metric, so an emitter that
    is misconfigured (or is leaking a token) stays just as visible as before.

    NOTE FOR CALLERS: the human-readable original is NOT recoverable from the
    return value any more — a slug now resolves to a uuid. Callers that display a
    tenant label must use :func:`original_tenant_label` on the RAW value instead
    of falling back to the returned id.

    ⛔ THE ``.strip()`` IS LOAD-BEARING — IT IS WHAT KEEPS THIS FUNCTION IN
    AGREEMENT WITH THE INGEST VALIDATOR. ``validate_cost_event`` decides
    admission with ``canonical_tenant_id(raw_tenant)``, and BOTH of those strip
    (``_cost_schema`` does ``str(raw_tenant).strip()``; ``canonical_tenant_id``
    strips again). This function did not. The two therefore disagreed about the
    SAME wire value, and the disagreement was silent and one-directional:

        tenant_id = " 019de842-…"   (or "n8n-dev\\n")
          validate_cost_event -> resolves a REAL tenant -> ADMITTED, not DLQ'd
          _normalize_tenant_id -> _parse_uuid fails on the padding, then
                                  looks_like_credential() returns True for ANY
                                  " ", "\\t" or "\\n" anywhere in the value
                               -> NIL_TENANT_ID, is_non_uuid=True

    A padded-but-perfectly-valid tenant was thereby booked to ``default-anon``.
    That is not merely a mis-attributed label: ``cost_flusher`` keys the token
    rollup and ``debit_budget`` off this
    return, and it dropped its old ``if tid == UNATTRIBUTED_TENANT_ID: continue``
    guard on the stated invariant that "what reaches here always resolves to a
    REAL tenant uuid" — an invariant only this strip makes true. So the real
    tenant went uninvoiced AND unenforced against its budget, and the spend
    re-accumulated in exactly the nil-tenant hole the 2026-08-26 contract change
    was written to close. Billing is append-only with no refund path, so every
    such row is permanently unbillable.

    Stripping does NOT weaken credential detection: a real leaked header still
    trips ``looks_like_credential`` on its ``bearer``/``basic ``/``token ``
    prefix, on its interior whitespace, or on the >48-char length bound.
    """
    s = "" if raw is None else str(raw).strip()
    parsed = _parse_uuid(s)
    if parsed is not None:
        # The nil uuid IS a valid uuid but is NOT a real tenant — it is the
        # emitters' default-tenant fallback. Flag it so the flusher excludes it
        # from billing side effects, exactly like a leaked credential.
        if parsed.int == 0:
            return NIL_TENANT_ID, True
        return str(parsed), False
    if _looks_like_credential(s):
        return NIL_TENANT_ID, True
    if not s:
        return NIL_TENANT_ID, True
    resolved = canonical_tenant_id(s)
    if resolved is not None:
        return resolved, False
    return NIL_TENANT_ID, True


def original_tenant_label(raw: Any) -> str:
    """The human-readable label for a raw emitted tenant_id, safe to store/log.

    :func:`_normalize_tenant_id` now returns a uuid for every input, so the
    original slug ("n8n-dev") would be lost if a caller kept deriving the display
    name from the id. This preserves it, with the same credential redaction the
    log path uses — a ``tenant_name`` column is displayed in dashboards, so a
    leaked ``Bearer`` token must not reach it either.

    An absent tenant keeps the historical ``"unknown"`` label. That string is
    honest HERE, in a free-text display column; what was wrong was using it as an
    identifier.

    Strips for the same reason :func:`_normalize_tenant_id` does, so the label
    and the id are derived from the same value: without it a padded slug either
    got redacted as a bogus "credential" or was stored with its padding into the
    ``tenant_name`` display column. A whitespace-only value is now "unknown"
    rather than a redaction of nothing.
    """
    s = "" if raw is None else str(raw).strip()
    if not s:
        return "unknown"
    if _looks_like_credential(s):
        return _redact_tenant_id(s)
    return s


def stored_tenant_name(
    tenant_id: Any,
    emitted_name: Any,
    raw_tenant_id: Any,
) -> tuple[str, bool]:
    """The display label to STORE on a row: ``(label, is_a_non_answer)``.

    THE ONE PLACE both write paths resolve a display name, because they had the
    same defect for the same reason. ``services.cost_flusher`` and
    ``services.event_flusher`` each carried the identical expression
    ``e.get("tenant_name","") or original_tenant_label(e.get("tenant_id",""))``,
    and since ``0011_tenant_id_is_a_uuid`` the emitters send a uuid as
    ``tenant_id`` and no ``tenant_name`` at all — so both faithfully preserved
    the UUID into a display column. Measured 2026-09-05:
    ``billing_cost_events.tenant_name`` is a uuid on 66,103 of 66,103 rows
    ($310.49), and ``billing_metering_events.tenant_name`` on 9 of 10 tenants /
    4,469 rows (billing-service #135). Two tables, one line of code, so one fix.

    ⛔ THE UUID IS WORSE THAN AN ABSENCE, which is why it is the ONE case coerced
    here. A uuid is non-empty and distinct per tenant, so it satisfies every
    downstream truthiness guard and reads as data: ``fr``'s metering page guards
    its id column with ``{#if t.tenant_name}`` and therefore printed the same id
    twice (narrowed server-side in fr#980, but the stored data is still wrong for
    every other consumer). ``""`` is the honest value and the only one available:
    ``billing_metering_events.tenant_name`` is ``NOT NULL`` with no default
    (alembic 0001 baseline), so NULL is not writable without a schema migration —
    and ``""`` reads identically at the display layer, because
    ``services.tenant_names.resolve_tenant_name`` already discards a stored name
    equal to the uuid and falls through to the uuid for rendering either way.

    ⚠️ EVERY OTHER SENTINEL IS PRESERVED, NOT COERCED. An emitter that literally
    sends ``"unknown"`` still gets ``"unknown"`` stored — see the
    ``billing_placeholder_value_total`` help text: nulling it out would move the
    laundering from "a fake value" to "an honest-looking absence" rather than end
    it. Such a value is flagged as a non-answer and counted, not rewritten. The
    uuid is different in kind: it does not merely stand in for a name, it
    IMPERSONATES one.

    Resolution order, matching ``resolve_tenant_name``'s read-time order exactly
    so the stored label and the rendered label cannot disagree:
      1. the live registry's name for this uuid,
      2. else a real name the emitter actually sent,
      3. else the label carried by the RAW emitted value (credential-redacted),
      4. and ``""`` when what that yields is merely the id echoed back.

    Never raises and never blocks: this runs once per event on both flush paths,
    and a display label must never be able to fail a stored money record.

    THE CONTRACT (decided 2026-09-06; do NOT "fix" it back)
    -------------------------------------------------------
    ``""`` is the CANONICAL "the registry could not resolve a name" value for
    ``billing_cost_events.tenant_name`` and ``billing_metering_events.tenant_name``.
    It does NOT mean "the name is empty". The columns stay ``NOT NULL``; we are
    not migrating that away, so ``""`` — not NULL — is the absence marker.

    A uuid must NEVER be written here. That is the defect this closes: a uuid
    asserts more than is known, while ``""`` asserts nothing.

    CONSUMERS: ``""`` is falsy, so ``if (name)`` / ``{#if name}`` are correct by
    default. ``name ?? id`` and ``COALESCE(tenant_name, …)`` are NOT — nullish
    coalescing and ``COALESCE`` both pass ``""`` straight through, so they must
    treat empty-or-whitespace as absent. (fr is adding exactly that narrowing:
    ``+page.svelte:151`` uses ``??`` and ``usage.ts:602``'s truthiness check both
    missed ``""``.)

    ⚠️ BOOT WINDOW, known and unsolved here. ``main.lifespan`` starts
    ``run_tenant_registry_refresh_loop`` as a background task and does not await
    its first tick, so rows minted before that tick — or during a licences
    outage — resolve to nothing and store ``""`` permanently. No backfill exists,
    and a later refresh cannot revisit a written row. Closing it means read-time
    resolution or a backfill job, NOT blocking this call on the registry: that
    would put a licences dependency on the money path, which is the one thing
    :mod:`tenancy`'s fail-soft design exists to prevent.
    """
    # Function-local for the same reason :func:`note_placeholder_value`'s is:
    # ``observability.metrics`` imports this package's ``_cost_schema``, so a
    # module-level import here risks a cycle. ``tenancy`` itself is pure.
    from tenancy import live_tenant_label

    registry_label = live_tenant_label(tenant_id)
    if registry_label:
        return registry_label, False

    label = str(emitted_name or "").strip() or original_tenant_label(raw_tenant_id)
    if label.strip().lower() == str(tenant_id).strip().lower():
        # The impersonation. Stored as an absence, and counted by the caller.
        return "", True
    if is_placeholder(label):
        # A sentinel the emitter chose. Kept verbatim, counted by the caller.
        return label, True
    return label, False


# ---------------------------------------------------------------------------
# Placeholder sentinels — countable, not laundered
# ---------------------------------------------------------------------------

#: Keys the cost flusher lifts off the wire into ``billing_cost_events.metadata``.
#:
#: Defined HERE rather than inline in ``cost_flusher`` so there is exactly ONE
#: list: :data:`PLACEHOLDER_COUNTED_FIELDS` below derives the counter's label
#: domain from it, and ``observability.metrics`` births a series per entry. A
#: hand-copied second list would let the whitelist grow a key whose placeholder
#: rate is then never born and never counted — invisible in precisely the way
#: this instrument exists to stop. The rationale for WHICH keys are on the list
#: (and why omitting one silently drops it) is the comment above the loop in
#: ``services.cost_flusher`` that consumes this.
COST_METADATA_WHITELIST: tuple[str, ...] = (
    "schema_id",
    "job_id",
    "fallback_used",
    "stage",
    "worker_name",
    "caller_client_id",
    # ``timestamp`` — the producer's own event time, parsed into the dedicated
    # ``billing_cost_events.event_ts`` column by ``cost_flusher`` (alembic 0028).
    #
    # IT IS ON THIS TUPLE FOR THE COUNTER, NOT FOR THE STORAGE. Entry here is the
    # ONLY way to get a per-``service_key`` measurement of whether an emitter
    # actually sends the field: ``PLACEHOLDER_COUNTED_FIELDS`` is DERIVED from
    # this tuple, so adding the key births
    # ``billing_placeholder_value_total{field="timestamp", service_key=…}`` and
    # the flusher's existing loop counts both halves — a sentinel value AND
    # absence. That matters because the claim "every emitter already sends
    # timestamp" is verified for only ten of the twelve live ``service_key``s:
    # ``w2v-video-analyzer`` and ``w2v-video-compiler`` (the #2 spender) could not
    # be confirmed to use the shared ``w2v_common`` cost bus. Until that series
    # reads zero for every service_key, "all emitters send it" is an assumption,
    # and this is the instrument that can refute it.
    #
    # The stored side effect is deliberate and useful, not redundant: the RAW wire
    # string survives in ``metadata`` even when ``event_ts`` comes out NULL
    # (unparseable, naive, skewed into the future), which is the only record of
    # WHAT the emitter actually sent. ``event_ts`` is the parsed answer; this is
    # the evidence. NB the counter cannot see an unparseable-but-non-placeholder
    # value ("2026-13-45" is not a sentinel word) — that class is counted on the
    # READ side instead, by ``rollup_engine``'s event-time fallback counter.
    "timestamp",
    # ``spend_scope`` — the emitter's own statement about what this spend is
    # attributed TO when it carries no ``project_id``. ``'tenant'`` means "this
    # cost belongs to the whole tenant", which is a COMPLETE attribution, not a
    # gap: ``services.attribution_audit._PROJECT_SPEND_COVERAGE_SQL`` reads
    # ``metadata->>'spend_scope' = 'tenant'`` to separate it from genuinely
    # unattributed spend in ``billing_cost_events_project_spend_usd``.
    #
    # ⛔ IT IS ON THIS TUPLE BECAUSE A KEY THAT IS NOT IS SILENTLY DROPPED.
    # inference-router writes ``spend_scope`` as a flat field on the cost-bus
    # event, and ``cost_flusher``'s loop lifts ONLY the keys named here. Without
    # this entry the field never reaches ``metadata``, the coverage query's
    # ``tenant_scoped`` bucket reads $0 in perpetuity, and the split reports "no
    # tenant-scoped spend exists" — indistinguishable from the emitter never
    # having sent one. That is the same failure the ``caller_client_id`` note in
    # ``cost_flusher`` records: whitelisting lands FIRST, before the producer.
    #
    # ⭐ AND THIS IS THE RIGHT PLACE RATHER THAN A SPECIAL CASE IN THE FLUSHER,
    # for exactly the reason ``timestamp`` above gives: ``PLACEHOLDER_COUNTED_FIELDS``
    # is DERIVED from this tuple, so membership here births
    # ``billing_placeholder_value_total{field="spend_scope", service_key=…}`` and
    # the flusher's existing loop counts BOTH halves — a sentinel value and
    # absence — per emitter. That is a free, per-``service_key`` answer to "is
    # this emitter actually sending a scope?", which is the question that will be
    # asked the moment anyone claims project-coverage numbers are complete. A
    # coverage figure with no per-emitter denominator is a share of an unknown.
    #
    # ⛔ NO COERCION AND NO DEFAULT, deliberately. An emitter that sends nothing
    # continues to send nothing, the key stays OUT of ``metadata`` entirely, and
    # the row lands in the ``unattributed`` bucket — which is the true reading.
    # Defaulting it to ``'tenant'`` (or to anything) would reproduce the
    # ``job_id = "unknown"`` defect memorialised two lines below in
    # ``cost_flusher``: 968 of 968 rows carrying a fabricated value that every
    # reader treated as populated. ``absent`` is a real state here and must stay
    # legible as one.
    "spend_scope",
    # ``delegated_by`` — the subject id inference-router VOUCHED for when it
    # honoured a delegated call: a SERVICE licence calling on behalf of an END
    # tenant, admitted only when the subject is in the router's
    # ``AUTH_DELEGATING_SUBJECTS``. Stamped ONLY on an honoured delegation, so
    # its presence is the router's own statement "license_id and tenant_id
    # differ on purpose". ``services.attribution_audit._WINDOW_SQL`` reads it to
    # count such rows as ``delegated`` rather than as a licence/tenant mismatch.
    #
    # ⛔ The audit honours it ONLY when ``cost_source = 'inference-router'``. It
    # is a wire field any emitter can send; from an emitter that never ran the
    # allowlist check it is noise and the row stays a mismatch. Bounded by
    # :data:`DELEGATED_BY_MAX_LEN` in the flusher — a subject id, not a payload.
    "delegated_by",
)

#: Upper bound on a stored ``delegated_by`` value. A Keycloak subject / client id
#: is well under this; anything longer, or not a string, is not a subject id and
#: is dropped by ``cost_flusher`` rather than stored.
DELEGATED_BY_MAX_LEN = 128

#: Keys the flusher STORES in ``billing_cost_events.metadata`` but deliberately
#: does NOT put on :data:`COST_METADATA_WHITELIST`, and therefore does not count
#: on ``billing_placeholder_value_total``.
#:
#: ⛔ WHY A SECOND LIST, despite the "exactly ONE list" note above: that note is
#: about keys whose ABSENCE is an emitter defect. Every whitelist key is counted
#: as a placeholder when missing, per ``service_key``. These keys are OPTIONAL BY
#: DESIGN — only conveyor (n8n) calls through inference-router carry them — so
#: their absence on every other emitter is the correct state, and counting it
#: would grow a series that reads "100% placeholder" for every non-conveyor
#: service_key forever: noise that teaches readers to ignore the counter.
#:
#: * ``card_id`` — the Planka card id the call was made for; read by
#:   ``GET /api/costs/projects/{project_id}`` as ``metadata->>'card_id'`` for
#:   ``by_card``. Without this entry the key is dropped and by_card is always the
#:   null bucket.
#: * ``execution_id`` — the n8n execution id; stored for correlation.
#:
#: Values go through :func:`parse_optional_metadata_str`: absent, non-string,
#: empty, sentinel or over-long values are dropped (not stored), so the row lands
#: in the null card bucket — the honest reading — rather than under a fabricated
#: card named "unknown" or a multi-KB payload.
OPTIONAL_COST_METADATA_FIELDS: tuple[str, ...] = ("card_id", "execution_id")

#: Upper bound on an :data:`OPTIONAL_COST_METADATA_FIELDS` value (after strip).
#: A Planka card id / n8n execution id is far shorter; longer is not an id.
OPTIONAL_COST_METADATA_MAX_LEN = 128


def parse_optional_metadata_str(val: Any) -> str | None:
    """Return a stripped id string, or None when it must not be stored.

    None for: not a ``str``, empty after strip, a placeholder sentinel
    (:func:`is_placeholder`), or longer than :data:`OPTIONAL_COST_METADATA_MAX_LEN`.
    Never counted — see :data:`OPTIONAL_COST_METADATA_FIELDS` for why.
    """
    if not isinstance(val, str):
        return None
    s = val.strip()
    if not s or len(s) > OPTIONAL_COST_METADATA_MAX_LEN or is_placeholder(s):
        return None
    return s


#: Columns the flusher DEFAULTS to the literal ``"unknown"`` when the wire
#: carries nothing. Each is NOT NULL in ``billing_cost_events`` (0001 baseline),
#: so the sentinel is not a stylistic choice — it is the only writable value.
DEFAULTED_COST_FIELDS: tuple[str, ...] = ("service_key", "provider", "cost_source")

#: Columns the flusher RESOLVES from the live tenant registry at write time,
#: and which fall back to a non-answer when the registry cannot vouch for the
#: value. Distinct from :data:`DEFAULTED_COST_FIELDS` because the fallback is not
#: a fixed ``"unknown"`` literal: ``tenant_name`` falls back to whatever the
#: emitter sent, which since migration 0011 is the tenant UUID echoed back — a
#: value that LOOKS populated and is why the column read as healthy on 66,103 of
#: 66,103 rows while carrying no name at all. Counted on the same series as the
#: other two families so "this field is not really populated" is one question
#: with one instrument, not three.
RESOLVED_COST_FIELDS: tuple[str, ...] = ("tenant_name",)

#: The bounded ``field`` label domain of ``billing_placeholder_value_total``.
PLACEHOLDER_COUNTED_FIELDS: tuple[str, ...] = (
    COST_METADATA_WHITELIST + DEFAULTED_COST_FIELDS + RESOLVED_COST_FIELDS
)

#: Values that are a stand-in for a value rather than one. Compared
#: case-insensitively against the STRIPPED string form, because an emitter that
#: writes "Unknown" or " unknown " is doing the same thing as one that writes
#: "unknown" and must not read as honest by virtue of its casing.
#:
#: This set holds WORDS only. Emptiness is handled separately in
#: :func:`is_placeholder`, because "" is not a spelling of "unknown" — it is
#: absence, and it must answer True without being listed among the sentinels a
#: human actually typed.
PLACEHOLDER_VALUES: frozenset[str] = frozenset(
    {"unknown", "unspecified", "none", "null", "nil", "n/a", "na", "-", "undefined", "default"}
)


def is_placeholder(val: Any) -> bool:
    """True if ``val`` stands in for a value rather than being one.

    Covers all three shapes of "this field never carried anything": ``None``, an
    empty/whitespace string, and a sentinel word. They are one fact about the
    emitter and belong on one series — a counter that only saw the WORDS would
    report a field nobody ever populates as 0% placeholder, i.e. perfect health.

    ⚠️ Emptiness answering True does NOT double-count in the flusher's metadata
    whitelist: that loop tests ``if c.get(k)`` first, so an empty value takes the
    ``else`` branch and is counted there, never reaching this function. It is the
    ``DEFAULTED_COST_FIELDS`` loop — which has no such truthiness test, because
    the coercion it counts IS ``or "unknown"`` — that needs "" to answer True.
    An earlier version excluded "" here and silently never counted an empty
    ``service_key``, which is the single most likely defaulted field in the set.
    """
    if val is None:
        return True
    stripped = str(val).strip()
    return not stripped or stripped.lower() in PLACEHOLDER_VALUES


def note_placeholder_value(field: str, service_key: str) -> None:
    """Count one stored field that carries a placeholder instead of a value.

    DOES NOT CHANGE THE VALUE. The sentinel is still written — see the
    ``billing_placeholder_value_total`` help text for why coercing it to NULL
    would move the laundering rather than end it. This only makes the
    fabrication countable, so ``metadata.job_id = 'unknown'`` on 968 of 968
    inference-router rows reads as the 100% emitter gap it is instead of as 968
    job ids.

    Every failure is swallowed, and the import is function-local, for the same
    two reasons as :func:`tenancy._note_unregistered_tenant`: an observability
    defect must never break the flush path, and ``observability.metrics``
    imports this package's ``_cost_schema``, so a module-level import here would
    be a circular import.
    """
    try:
        from observability.metrics import placeholder_value_total

        placeholder_value_total.labels(field=field, service_key=service_key).inc()
    except Exception:  # pragma: no cover - observability must not break the flush
        pass


def note_metering_placeholder_value(field: str, service_key: str) -> None:
    """The metering-stream twin of :func:`note_placeholder_value`.

    A SEPARATE series, not a widening of the cost one, and the separation is the
    point. ``billing_placeholder_value_total``'s help text names its denominator
    — ``billing_cost_events_by_service_total`` — so a metering row counted there
    would put two populations on one series and make every rate taken against
    that denominator wrong. One metric name must not carry two meanings.

    Same swallow-everything, function-local-import contract as its twin: an
    observability defect must never break a flush.
    """
    try:
        from observability.metrics import metering_placeholder_value_total

        metering_placeholder_value_total.labels(field=field, service_key=service_key).inc()
    except Exception:  # pragma: no cover - observability must not break the flush
        pass


#: The bounded ``field`` label domain of ``billing_metering_placeholder_value_total``.
#: Only ``tenant_name`` for now — the metering row has no metadata whitelist and
#: no ``or "unknown"`` defaulted columns, so this is deliberately not the same
#: domain as the cost counter's.
METERING_PLACEHOLDER_COUNTED_FIELDS: tuple[str, ...] = ("tenant_name",)


def _parse_decimal(val: Any) -> Decimal:
    try:
        return Decimal(val)
    except (InvalidOperation, TypeError, ValueError):  # fmt: skip
        return Decimal("0")


def _parse_optional_decimal(val: Any) -> Decimal | None:
    """Like :func:`_parse_decimal`, but absence stays absence.

    ``_parse_decimal`` coerces a missing/unparseable value to ``Decimal("0")``,
    which is correct for a field billing always books (e.g. ``cost_usd_hint``)
    but wrong for one that means "unknown" until an emitter actually reports
    it (``vendor_cost_usd`` — alembic 0021). ``0`` there would assert "the
    vendor charged nothing", a claim the wire never made.
    """
    if val is None or val == "":
        return None
    try:
        return Decimal(val)
    except (InvalidOperation, TypeError, ValueError):  # fmt: skip
        return None


def _parse_int(val: Any) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):  # fmt: skip
        return 0


#: How far BEFORE ingest a producer-asserted event time may sit and still be
#: usable. Tied to ``Settings.retention_days`` (default 90) and the catch-up
#: window it bounds (``services.aggregator.MAX_CATCHUP_DAYS`` = 92): a bucket date
#: older than retention is never rolled by anything — ``catchup_daily`` and
#: ``missing_daily_dates`` are both floored at retention — so accepting a wildly
#: back-dated ``event_ts`` would route the row to a bucket NOTHING will ever
#: aggregate, i.e. silently unbilled revenue. Refusing it falls back to
#: ``created_at``, which bills the row on its ingest day: late, but billed.
#:
#: A CONSTANT, not a setting, on purpose — this module is imported by
#: ``observability.metrics`` (see :func:`note_placeholder_value`) and must not
#: pull ``settings`` into that cycle. The cost of the two drifting is
#: one-directional: raise ``retention_days`` above 90 and a 91-day-old event time
#: is billed on its ingest day instead of its event day. Wrong day, never lost.
MAX_EVENT_TS_BACKDATE_DAYS: int = 90


def parse_event_ts(val: Any, *, received_at: datetime) -> datetime | None:
    """Parse a producer-asserted event time, or return ``None``. Never raises.

    ``None`` is the CORRECT, load-bearing answer for anything this function
    cannot vouch for: the caller stores NULL in
    ``billing_cost_events.event_ts``, and every reader falls back to
    ``created_at`` for a NULL row and COUNTS the fallback. So the rule here is
    "refuse, never guess" — a guessed event time silently moves money between
    calendar months, and the fallback is a known-true ingest instant.

    Accepted: an ISO-8601 string with an EXPLICIT offset, as every measured
    emitter sends (``datetime.now(UTC).isoformat()`` →
    ``2026-08-31T23:59:50.123456+00:00``; a trailing ``Z`` is accepted too), or an
    already-aware :class:`datetime` (a direct in-process caller / test).

    Refused, and why each refusal is the honest answer rather than a limitation:

    * missing / empty / whitespace — nothing was asserted.
    * not ISO-8601 — a malformed instant is not an instant.
    * NAIVE (no offset) — ambiguous. The producer's local zone is unknown to
      billing, and stamping the wrong one on a month boundary moves a charge
      into the wrong INVOICE. All three measured emitters send an offset, so
      this costs nothing today; an emitter that starts sending naive values
      shows up as fallback instead of as a silently mis-bucketed charge, and the
      fix belongs at the emitter (``docs/EMITTER_GUIDE.md``).
    * a number (epoch) — ambiguous between seconds and milliseconds, and
      guessing wrong is a ~50,000× error in the bucket date. No emitter sends
      one; if one ever does, add an explicit, tested branch rather than a sniff.
    * AFTER ``received_at`` — a skewed producer clock. Billing must not book a
      cost into the future, and this refusal is also what keeps
      ``rollup_daily``'s sargable lower bound on ``created_at`` SOUND: because
      every stored ``event_ts`` is ``<= received_at <= created_at``, a row whose
      resolved event date is ``D`` always has ``created_at >= D`` — which is why
      that query can still prune partitions on the partition key. ⛔ Relaxing
      this refusal silently breaks that bound and rows go missing from rollups.
    * more than :data:`MAX_EVENT_TS_BACKDATE_DAYS` before ``received_at`` — see
      that constant: such a bucket is never rolled by anything, so honouring it
      would mean never billing the row at all.

    The return value is always normalised to UTC, so the caller binds one
    unambiguous instant regardless of the offset the producer used.
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        parsed = val
    elif isinstance(val, str):
        raw = val.strip()
        if not raw:
            return None
        try:
            # 3.11+ fromisoformat accepts a trailing "Z"; normalise anyway so the
            # behaviour does not depend on that detail.
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00").replace("z", "+00:00"))
        except (ValueError, TypeError):  # fmt: skip
            return None
    else:
        # Includes int/float epoch values — deliberately refused, see above.
        return None

    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        return None
    parsed = parsed.astimezone(UTC)

    if parsed > received_at:
        return None
    if parsed < received_at - timedelta(days=MAX_EVENT_TS_BACKDATE_DAYS):
        return None
    return parsed


def _parse_usage_extra(val: Any) -> dict | None:
    """Parse usage_extra which may arrive as a JSON string or already a dict.

    Returns None on parse failure or empty input — JSONB column accepts NULL.
    """
    if val is None or val == "":
        return None
    if isinstance(val, dict):
        return val or None
    if isinstance(val, (bytes, bytearray)):
        try:
            val = val.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
        except json.JSONDecodeError, ValueError:
            return None
        if isinstance(parsed, dict):
            return parsed or None
    return None


# --------------------------------------------------------------------------
# event_key derivation at the accept seam
# --------------------------------------------------------------------------

#: How the ``event_key`` on a stored cost event came to be. Enumerated here so
#: the ``billing_cost_event_key_source_total`` label domain is DERIVED from this
#: tuple rather than hand-copied beside it — the same arrangement
#: ``PLACEHOLDER_COUNTED_FIELDS`` uses, for the same reason: an absent series is
#: indistinguishable from a healthy one.
EVENT_KEY_SOURCES: tuple[str, ...] = ("producer", "provider_request_id", "absent")

#: Namespace prefix on a DERIVED key, so a key billing minted can never be
#: mistaken for one a producer stamped. Producer keys are uuid7 strings; a
#: derived key is ``d1:<32 hex>``. Both fit ``VARCHAR(64)``.
_DERIVED_KEY_PREFIX = "d1:"

#: Namespace prefix on a producer key that did not FIT and had to be condensed
#: (``p1:<32 hex>``). Distinct from ``d1:`` so the three provenances — stamped,
#: condensed, derived — stay tellable apart in the stored data.
_CONDENSED_KEY_PREFIX = "p1:"

#: Width of ``billing_cost_events.event_key`` AND of the
#: ``processed_cost_event_keys`` primary key — both ``VARCHAR(64)`` (migration
#: 0013). A key wider than this is not storable, so it must be condensed BEFORE
#: it reaches either table; see rung 1 for why it is condensed and not cut.
_EVENT_KEY_MAX_LEN = 64


def derive_event_key(fields: Any) -> tuple[str | None, str]:
    """Return ``(event_key, source)`` for one emitted cost event.

    ⛔ READ THIS BEFORE ADDING A RUNG. The obvious "fix" for a producer that
    sends no key — hash the call's CONTENT (tenant + model + tokens + cost) —
    is catastrophically wrong here, and the ledger says so out loud. Measured on
    the prod billing primary over 2026-09-05T07:01:58Z … 2026-09-06T08:50:30Z:

        26,283 mlx-backend rows share the signature
        (mlx-nlp, en_core_web_trf, input_tokens=30, cost_usd=0.00003) exactly,
        with 66 distinct latency_ms values between them and NOTHING else on the
        wire to tell them apart.

    Those are 26,283 genuinely distinct billable NLP calls. A content hash would
    claim the first and silently DROP the other ~26,282 — it would not fix
    double-billing, it would invent catastrophic UNDER-billing, which is the
    same defect pointed the other way and far harder to notice. A key may only
    be derived from an input that is unique PER CALL and stable ACROSS A
    REDELIVERY of that call. Content is neither.

    Rungs, in order:

    1. ``event_key`` on the wire → used verbatim. Producers that already mint
       one (worker_management/inference-router, and all four w2v emitters) are
       untouched by this function. Two guards, both of which exist because the
       failure they prevent is a SILENT merge of distinct billable calls rather
       than a visible error:

       * a placeholder key is not an identity. ``is_placeholder`` is the same
         test rung 2 applies to ``provider_request_id``, and rung 1 must not be
         the softer of the two: honouring a literal ``"unknown"`` verbatim
         would put every such row on one key and bill one of them. A sentinel
         key falls THROUGH to rung 2, so such a row is still stored and still
         billed — it simply has to ground its identity in something real.
       * an over-long key is condensed, never cut. See the rung-1 body.
    2. ``provider_request_id`` → the upstream provider's own per-call id. Unique
       per call and identical on a re-emit of that call, so hashing it with the
       tenant and service yields a key with exactly the guarantee rung 1 has.
       This is the rung that does new work: it covers the 1,250 keyless
       inference-router rows measured in that window, all of which carry a real
       provider_request_id.
    3. Nothing usable → ``(None, "absent")``. The row is stored and billed
       exactly as today; it simply carries no per-call idempotency guarantee,
       because the producer supplied nothing that could ground one. Returning
       NULL is the HONEST answer and a minted-per-emit uuid would be a lie: it
       would satisfy a NOT NULL constraint, claim a row in the inbox, and dedupe
       nothing at all, while making the emitter gap invisible.

    ⚠️ ``correlation_id`` is deliberately NOT a rung. A correlation id spans a
    whole request chain — kag emits many stage events under one — so keying on
    it would collapse a multi-step DAG into its first step.

    The caller counts the returned ``source``, so the population on rung 3 is
    legible per producer instead of being a silent NULL. That counter is the
    arming criterion for ever making the key REQUIRED: over the window above,
    rung 3 is 82.6% of all rows (58,249 / 70,492), so a rejection today would
    refuse five sixths of production traffic. That is an outage, not a fix.
    """
    raw = fields.get("event_key")
    if not is_placeholder(raw):
        key = str(raw).strip()
        if len(key) > _EVENT_KEY_MAX_LEN:
            # ⛔ NOT ``key[:_EVENT_KEY_MAX_LEN]``. Two distinct producer keys
            # that agree on their first 64 characters would cut down to ONE
            # stored key, and the second call would be dropped as a replay of
            # the first — silent UNDER-billing. That is the same defect the
            # content hash was rejected for above, reached by a different route.
            #
            # REJECTED ALTERNATIVE — raise on an over-long key. A loud failure
            # is the right shape for a defect that would otherwise be silent,
            # but not at THIS seam: derive_event_key is called per row inside a
            # flush batch, so one malformed row would abort the batch and every
            # legitimate charge travelling with it. Dropping billable rows to
            # punish a producer's formatting is the outage this function already
            # refuses for keyless producers (rung 3). Condensing keeps the row,
            # keeps the guarantee — sha256 is deterministic across a redelivery
            # and injective in practice, which is exactly what rung 1 needs —
            # and gives up only the key's readability. Widening the column was
            # not an option either: the key is a PRIMARY KEY in
            # processed_cost_event_keys, and a producer can send any length.
            digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]
            key = f"{_CONDENSED_KEY_PREFIX}{digest}"
        return key, "producer"

    prid = fields.get("provider_request_id")
    if not is_placeholder(prid):
        tenant = str(fields.get("tenant_id") or "")
        service = str(fields.get("service_key") or "")
        # "\x1f" (unit separator) cannot occur in any of the three parts, so the
        # join is unambiguous: a tenant ending in "|" plus an empty service can
        # never collide with the next tenant along, the way a "|" join allows.
        material = "\x1f".join((tenant, service, str(prid).strip()))
        digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
        return f"{_DERIVED_KEY_PREFIX}{digest}", "provider_request_id"

    return None, "absent"


def note_event_key_source(service_key: str, source: str) -> None:
    """Count how one stored cost event's ``event_key`` was obtained.

    Function-local import and a swallowed failure for the same two reasons as
    :func:`note_placeholder_value`: an observability defect must never break the
    flush path, and ``observability.metrics`` imports this package.
    """
    try:
        import observability.metrics as _m

        _m.cost_event_key_source_total.labels(service_key=service_key, source=source).inc()
    except Exception:  # pragma: no cover - observability must never break billing
        pass
