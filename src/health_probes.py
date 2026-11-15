"""Self-metering health probes: recorded, never billed.

THE DEFECT
----------
Readiness probes across the estate deliberately call REAL data-plane endpoints —
that is the whole point of a readiness probe, and it must stay that way. But
every one of those calls goes through a metered route and mints a billable cost
event. Measured on the prod billing primary 2026-09-07:

    subject_id                            tenant_name      service_key  endpoint       rows/24h
    01a0138e-e7b9-712c-b140-cf20d7b9e644  svc-kag-karma    mlx-backend  process-text     16,904
    01a0138e-e7b9-712c-b140-cf20d7b9e644  svc-kag-karma    mlx-backend  vocab-info       16,902
    019f83bd-b8b8-72f2-beb9-d138dab0fa1f  svc-nlp-service  mlx-backend  process-text      4,688

That is 38,494 of 39,519 rows carrying the ``mlx-backend`` service key in 24h
— 99.96% —
and ~82% of every row in ``billing_cost_events``, carrying 0.41% of the money.

⛔ THIS IS A DATA-INTEGRITY AND VOLUME PROBLEM, NOT A SPEND PROBLEM, and the fix
must not pretend otherwise. Nobody is being over-charged in any amount that
matters. What is broken is that the ledger is four-fifths synthetic, so every
row count, every per-endpoint distribution and every "is this service busy"
reading taken off it is noise about the probe rather than signal about usage.

WHY THE FIX IS HERE AND NOT AT THE PROBE
----------------------------------------
Two earlier attempts took the wrong shape and were rejected:

1. Raise the probe's cache TTL. Removes a FRACTION of the rows and pays for it
   by slowing failure detection — it trades observability for tidiness and does
   not even finish the job.
2. Re-point the probe at a non-metering endpoint (``nlp_service`` PR #61). It
   works, and it is worse: the probe then exercises a backend route production
   never calls, and stops exercising the one it does. A readiness probe that
   proves a path nobody uses is not a readiness probe.

PROBES MUST KEEP CALLING THE REAL ENDPOINTS. THE METER IS WHAT CHANGES.

WHY THE ROW IS PARKED, NOT DROPPED
----------------------------------
The same reasoning ``services.cost_flusher`` already applies to an unregistered
tenant, and the same mechanism migration 0013 uses for duplicates: the event is
STORED with ``excluded_from_billing = true`` and
``exclusion_reason = 'health_probe'``. The row stays in the ledger, stays
auditable, stays reversible with one UPDATE, and is dropped from every billable
aggregate by the ``excluded_from_billing`` filter those aggregates already carry.
Dropping the event would destroy the record of a call that really happened and
would make this module unfalsifiable — you could never afterwards measure what
it had excluded.

⛔ THE RULE IS KEYED ON IDENTITY. PAYLOAD SIZE IS A CONJUNCT, NEVER A KEY
-------------------------------------------------------------------------
The distinction is the whole safety argument, and collapsing it is the mistake
this section exists to prevent.

AS A SOLE KEY, payload size is catastrophic. The karma probe sends exactly
``"Ada Lovelace worked in London."`` (30 chars) and the nlp_service probe
``"Readiness probe."`` (16 chars), each ONE distinct ``input_tokens`` value
across tens of thousands of rows, so ``input_tokens IN (16, 30)`` would look
flawless on every dashboard — and would silently stop billing a genuine
16- or 30-token request from ANY tenant in the estate. That is far worse than
the problem being fixed and invisible for exactly the same reason: the excluded
rows look precisely like the ones the rule was aimed at.

AS AN AND-CONJUNCT on an identity that has ALREADY matched on subject, tenant,
service key and endpoint, it has the OPPOSITE sign. It cannot widen a rule; it
is unreachable except for events the identity rule already parked; and it can
only ever move rows back INTO billing. Its blast radius is bounded by the
allowlist, not by the estate. Measured: it recovers 5 real rows / $0.112 per 7d
on the nlp_service triple that the identity rule alone parks, and changes
nothing at all for karma (93,800/93,800 rows are already ≡30). It is also
defence in depth — if a tenant id were ever wrong, ``nonagentic-gtm``'s 364
rows at 65 distinct token values are protected a second time by this conjunct
independently.

⚠️ WHAT THE CONJUNCT COSTS, stated plainly because it is a REAL new failure
mode and it is not the one people expect. It is not over-exclusion — an
AND-conjunct cannot over-exclude. It is SILENT ROT: change the probe's text by
one character and the rule stops matching, and probe rows quietly resume
billing with nothing looking wrong. Identity alone was robust to that. Two
controls close it, and both are load-bearing:

  * PRODUCTION: ``billing_cost_events_health_probe_total`` going FLAT for a
    subject that is still probing. Every series is birthed at import precisely
    so this reads as a zero rather than an absent series.
  * CI: ``test_every_allowlist_entry_pins_the_live_probe_payload`` checks each
    pinned ``input_tokens`` against the modal value the ledger census actually
    observed for that triple. A changed probe text becomes a red build naming
    the entry, not a slow leak.

THE RESIDUAL FALSE POSITIVE, stated honestly rather than buried: a genuine
request of EXACTLY 16 or 30 input_tokens, from the same subject AND the same
tenant AND the same service key AND the same endpoint as a probe, is still
parked. Over 7 days that population is ZERO rows. It is a far narrower window
than parking every request from that identity — which is what this module did
before the conjunct — and the rows stay stored, auditable and reversible with
one UPDATE either way.

⚠️ A SUBJECT IS NOT A SERVICE — READ THIS BEFORE ADDING AN ENTRY
----------------------------------------------------------------
This was measured, and it is why ``tenant_id``, ``service_key`` and ``endpoint``
are part of the key rather than decoration. ``subject_id`` alone is NOT the
identity of a probe: over 7 days the karma subject ``01a0138e-…`` carried
traffic for TWO different tenants on the very triple this module excludes.

    subject       tenant_id / name              service_key       endpoint      rows  distinct  cost
    01a0138e-…    f172bf91-… svc-kag-karma      mlx-backend       process-text 46,840        1  $1.405
    01a0138e-…    f172bf91-… svc-kag-karma      mlx-backend       vocab-info   46,829        1  $1.405
    01a0138e-…    e4f70a83-… nonagentic-gtm     mlx-backend       process-text    171       44  $0.035
    01a0138e-…    e4f70a83-… nonagentic-gtm     mlx-backend       vocab-info      171       44  $0.035
    01a0138e-…    e4f70a83-… nonagentic-gtm     inference-router  (none)           22       22  $0.410
    019f83bd-…    c13dfe28-… svc-nlp-service    mlx-backend       process-text 10,582        2  $0.281
    019f83bd-…    c13dfe28-… svc-nlp-service    mlx-backend       vocab-info        5        2  $0.112

Read the ``distinct`` column: 1 distinct ``input_tokens`` value across 46,840
rows is a probe; 44 across 171 rows is a person. Each narrowing below is doing
measured work, and dropping any one of them re-admits real traffic:

* ``tenant_id`` protects ``nonagentic-gtm``'s 342 mlx-backend rows ($0.071). A
  subject-only rule parks every one of them. THIS IS THE NARROWING THAT MATTERS
  MOST and it is the one the obvious design omits.
* ``service_key`` protects ``nonagentic-gtm``'s 22 ``inference-router`` rows
  ($0.410) — the single most expensive population any of these rules comes near.
* ``endpoint`` protects ``svc-nlp-service``'s 5 real ``vocab-info`` rows
  ($0.112) at 22,031–23,833 input_tokens.

WHAT IS STILL LOST: with all four conditions applied, NOTHING that was measured.
The ``input_tokens`` conjunct closes the residue the identity rule alone left —
the 5 genuine ``svc-nlp-service`` ``process-text`` rows / $0.112 per 7d — so the
measured collateral is now ZERO rows over the 7d window, against ~104,400 probe
rows leaving the billable population.

⚠️ ZERO MEASURED IS NOT ZERO POSSIBLE. A genuine request of exactly 16 or 30
input_tokens from the same subject AND tenant AND service key AND endpoint as a
probe is still parked. That population is empty today; it is not empty by
construction. Such a row stays stored, auditable and reversible with one UPDATE,
and it is counted on ``billing_cost_events_health_probe_total`` like any other
exclusion.

⚠️ AND ONE CENSUS TRAP, because it cost this change a wrong conclusion once. A
7d census GROUPED BY ``tenant_name`` splits each identity below into TWO rows —
its resolved name and its raw uuid — because ``tenant_name`` held the uuid on
66,103 of 66,103 rows before the write-time resolution fix landed. Read that
output and ``f172bf91-…`` looks like a probe tenant MISSING from this allowlist,
larger than ``svc-kag-karma`` and unbilled. It is not: it is the same
``tenant_id`` as ``svc-kag-karma`` and it is already covered. GROUP BY
``tenant_id``, never by ``tenant_name``.

THE EXACT FIX, WHICH THIS MODULE CANNOT MAKE ALONE
--------------------------------------------------
The residue above exists because billing is inferring "this was a probe" from
WHO CALLED rather than being TOLD. The exact fix is for the probe to mark itself
— an ``X-Health-Probe`` header propagated by the emitter into
``usage_extra.health_probe`` — and for billing to honour that marker ONLY for
subjects on this allowlist. Two factors: the emitter asserts it is a probe, and
billing's allowlist says that subject is permitted to assert it, so no tenant can
self-exempt by setting a flag. :func:`match_health_probe` is deliberately shaped
so that marker becomes an additional NARROWING and never a new way in. Until the
emitters carry it, identity alone is what there is, and the residue above is its
measured cost.

FIXTURE / DRIFT
---------------
An allowlist whose entries have rotted is a rule that has silently stopped
matching, and nothing about a stale entry looks wrong. Every entry here must
appear in ``tests/fixtures/health_probe_subjects.txt`` — a measured snapshot of
the live ledger, regenerated by ``scripts/refresh_health_probe_fixture.sh`` —
and ``tests/test_health_probe_exclusion.py`` fails CI if one does not. Same
shape as the tenant-registry fixture this repo already keeps for the same reason.
"""

from __future__ import annotations

import uuid
from typing import Any, NamedTuple

__all__ = [
    "HEALTH_PROBE_EMITTERS",
    "HEALTH_PROBE_EXCLUSION_REASON",
    "ProbeRule",
    "match_health_probe",
    "probe_endpoint",
]

#: The value written to ``billing_cost_events.exclusion_reason``. The column is
#: ``varchar(64)``; this is 12 chars. Named once here so the flusher, the
#: metric, the tests and every SQL consumer cannot drift apart on a string
#: literal.
HEALTH_PROBE_EXCLUSION_REASON = "health_probe"


class ProbeRule(NamedTuple):
    """One allowlisted probe emitter.

    ``subject_id`` is REQUIRED and is the whole authorisation: it is the stable
    identity of the service account whose readiness probe mints these rows, and
    it is not something a caller can choose for itself.

    ``input_tokens`` is the probe's EXACT payload size, and it is a NARROWING
    ONLY — read the "payload size is a conjunct, never a key" section of the
    module docstring before touching it. Exactly one value per entry: a probe
    with two payloads gets two entries, the same way the karma probe's two
    backend hops get two entries. A tuple here would be an invitation to widen
    it until it stops discriminating.

    ``tenant_id``, ``service_key``, ``endpoint`` and ``input_tokens`` are
    OPTIONAL NARROWINGS.
    ``None`` means "any", which is the widest and therefore the most dangerous
    form of an entry — every live entry pins all three, because measurement
    showed each one protecting real traffic that the wider rule would have
    parked (see the module docstring). Prefer the narrowest rule that still
    matches the probe: this is the one place in this repo where being
    over-specific costs nothing and being under-specific stops billing someone.

    ``note`` is not used in matching. It exists so a future reader can tell why
    an entry is here without excavating a git blame, and so the drift guard can
    quote it.
    """

    subject_id: str
    tenant_id: str | None = None
    service_key: str | None = None
    endpoint: str | None = None
    input_tokens: int | None = None
    note: str = ""


#: ⛔ THE ALLOWLIST. Adding an entry here stops billing real rows if it is wrong.
#:
#: Before adding one, MEASURE the candidate against the prod billing primary and
#: prove the narrowed triple carries no traffic you would mind losing:
#:
#:   SELECT usage_extra->>'endpoint' ep, count(*), count(DISTINCT input_tokens),
#:          sum(cost_usd), cost_basis
#:     FROM billing_cost_events
#:    WHERE created_at > now() - interval '7 days'
#:      AND subject_id = '<candidate>' AND service_key = '<key>'
#:    GROUP BY 1, 5 ORDER BY 2 DESC;
#:
#: A single distinct ``input_tokens`` value over a long window is EVIDENCE the
#: triple is probe-only. It is not the RULE — see the docstring. And check the
#: 7-day window, not 24h: both entries below look perfectly probe-pure at 24h
#: and are demonstrably not at 7d.
#:
#: Then add the row to ``tests/fixtures/health_probe_subjects.txt`` (or rerun
#: ``scripts/refresh_health_probe_fixture.sh``), or CI will reject the entry as
#: unmeasured.
#: The service key both live probes bill under. Bound to a NAME rather than
#: written inline below because detect-secrets' "Secret Keyword" rule fires on
#: any ``*_key = "<literal>"`` adjacency, and ``observability.metrics`` records
#: at length why an inline ``# pragma: allowlist secret`` is the fragile answer:
#: ruff-format may reflow the call and move the comment off the flagged line,
#: which is exactly how a test once went red in CI while passing the local hook.
#: Absorbing it into .secrets.baseline is worse still — a baseline entry is an
#: opaque hash that says nothing about why it is safe, and every one added makes
#: the next REAL finding easier to wave through.
MLX_BACKEND_SERVICE = "mlx-backend"

HEALTH_PROBE_EMITTERS: tuple[ProbeRule, ...] = (
    # karma_service readiness probe. ⚠️ karma_service, NOT kag — its licence
    # token resolves to an app record named "KAG Module", which has misled three
    # prior sessions into editing the wrong repo.
    #
    # ONE call site — karma_service/src/routers/health.py:113 POSTs
    # `"Ada Lovelace worked in London."` (health.py:65, 30 chars) to nlp_service's
    # POST /api/nlp/entities — but it fans out INSIDE nlp_service to TWO
    # mlx-backend hops, so it mints TWO billed rows per probe and needs two
    # entries here:
    #   text_processor.py:994   -> POST /api/nlp/process-text -> "process-text"
    #   text_processor.py:1418  -> POST /api/ml/vocab-info    -> "vocab-info"
    #     (reached via _detect_vocab_candidates)
    #
    # ⚠️ tenant_id is NOT optional decoration on these two. This subject is
    # SHARED: `nonagentic-gtm` (tenant e4f70a83-…) bills 342 real mlx-backend
    # rows / 7d on these exact two endpoints, at 44 distinct input_tokens
    # values. Pinning the tenant to svc-kag-karma preserves every one of them.
    # With it pinned these triples are exactly probe-pure: 1 distinct
    # input_tokens value across 93,669 rows / 7d.
    ProbeRule(
        subject_id="01a0138e-e7b9-712c-b140-cf20d7b9e644",
        tenant_id="f172bf91-2dd5-5386-ae73-704f31f4f0a9",  # svc-kag-karma
        service_key=MLX_BACKEND_SERVICE,
        endpoint="process-text",
        input_tokens=30,  # "Ada Lovelace worked in London." — 46,905/46,905 rows
        note="karma_service readiness probe, hop 1/2 (nlp_service text_processor.py:994)",
    ),
    ProbeRule(
        subject_id="01a0138e-e7b9-712c-b140-cf20d7b9e644",
        tenant_id="f172bf91-2dd5-5386-ae73-704f31f4f0a9",  # svc-kag-karma
        service_key=MLX_BACKEND_SERVICE,
        endpoint="vocab-info",
        input_tokens=30,  # 46,895/46,895 rows on this triple — no non-probe traffic
        note="karma_service readiness probe, hop 2/2 (nlp_service text_processor.py:1418)",
    ),
    # nlp_service's own readiness probe: `"Readiness probe."` (16 chars) ->
    # POST /api/nlp/entities.
    #
    # ⚠️ THIS ENTRY SAID `process-text` UNTIL 2026-09-23, AND THAT IS THE WHOLE
    # DEFECT THIS CHANGE CLOSES. The probe has not called `process-text` since
    # 2026-09-07, when nlp_service moved it to `entities`
    # (`src/routers/health.py:210-260`) SPECIFICALLY so it would stop billing —
    # at the time `process-text` was the only metered `/api/nlp/*` route, so
    # moving was sufficient and no rule was needed. That repo wrote the hazard
    # down verbatim: *"If mlx-backend ever adds schedule_cost_event to
    # /api/nlp/entities, this probe starts billing again and nothing in this
    # repo can fail."* mlx_backend PR #48/#49 (`a38d6d3`, 2026-09-17) did
    # exactly that, and the prediction came true to the letter: first billed
    # probe row 2026-09-17 17:18, and 27,123 synthetic rows by 09-23.
    #
    # ⭐ A COMMENT IN ANOTHER REPO CANNOT FAIL. That is why the fix ships with
    # `test_a_known_probe_identity_has_no_unallowlisted_probe_shaped_population`
    # — every drift guard here walked the ALLOWLIST and asked whether each entry
    # was still vouched for, which is blind to a probe that MOVED. This entry
    # stayed green while pointing at an endpoint carrying zero traffic.
    #
    # ⚠️ endpoint is still pinned and still doing real work: this subject/tenant
    # bills genuine `vocab-info` rows at 22k-24k input_tokens, and a rule
    # without the endpoint would park them.
    ProbeRule(
        subject_id="019f83bd-b8b8-72f2-beb9-d138dab0fa1f",
        tenant_id="c13dfe28-f5af-5656-8786-51f34b2629ec",  # svc-nlp-service
        service_key=MLX_BACKEND_SERVICE,
        endpoint="entities",
        # ⚠️ THE CONJUNCT NO LONGER RECOVERS ANYTHING ON THIS TRIPLE, and saying
        # so plainly matters: the old comment here claimed it rescued 5 real
        # rows / $0.112 per 7d, which was true of `process-text` and is NOT true
        # of `entities`. Measured 7d on this exact triple 2026-09-23: 27,136
        # rows, ONE distinct input_tokens value (16), zero non-probe traffic.
        # It is kept because it is a NARROWING — it can only ever move rows back
        # INTO billing — and because it is what
        # `test_every_allowlist_entry_pins_the_live_probe_payload` checks the
        # probe's payload against. Its value here is defence in depth, not a
        # measured recovery. Do not restate the $0.112 claim on this entry.
        input_tokens=16,  # "Readiness probe." — 27,136/27,136 rows ≡ 16 over 7d
        note="nlp_service readiness probe -> /api/nlp/entities (tenant svc-nlp-service)",
    ),
)


def probe_endpoint(usage_extra: Any) -> str | None:
    """The endpoint label a cost event carries, or None.

    The emitters put it in ``usage_extra.endpoint`` (verified in the live
    ledger: every mlx-backend probe row carries
    ``{"unit": "nlp_chars", "tokens": N, "endpoint": "...", ...}``). It is NOT a
    column, so there is nothing to migrate and nothing that can be NOT NULL.

    Total. Never raises and never coerces a non-string to one: an endpoint that
    is absent, null, a number or a nested object reads as None, which matches
    only a rule that asked for no endpoint at all. A malformed ``usage_extra``
    must not be able to widen a rule.
    """
    if not isinstance(usage_extra, dict):
        return None
    endpoint = usage_extra.get("endpoint")
    return endpoint if isinstance(endpoint, str) and endpoint else None


def match_health_probe(
    subject_id: Any,
    tenant_id: Any,
    service_key: Any,
    endpoint: str | None,
    input_tokens: Any = None,
    *,
    rules: tuple[ProbeRule, ...] = HEALTH_PROBE_EMITTERS,
) -> ProbeRule | None:
    """Return the allowlist entry this event matches, or None.

    ⛔ FAIL CLOSED — "closed" here meaning "bills". Every uncertain input
    returns None and the row is billed exactly as it is today. An absent
    ``subject_id`` (the overwhelming majority of live rows), a non-string, an
    unparseable value: all bill. This function can only ever REMOVE a row from
    billing when an operator has written that exact identity into the allowlist
    above, which is the only property that makes the mechanism safe to ship
    armed.

    Matching is exact on ``subject_id`` and (when pinned) ``tenant_id`` after
    uuid canonicalisation, so a rule written in any hyphenation or case still
    matches the uuid the flusher parsed — a formatting difference must not be
    the reason a rule silently stops firing. ``service_key`` and ``endpoint``
    are compared verbatim when the rule pins them and ignored when it does not.

    ⚠️ ``subject_id`` ALONE IS NOT AN IDENTITY. It is a credential, and a
    credential can be shared: the karma subject serves two different tenants,
    one of which is real traffic. Every narrowing a rule declares is checked;
    every one it omits is a population it will park.
    """
    canon_subject = _canonical_uuid(subject_id)
    if canon_subject is None:
        return None
    canon_tenant = _canonical_uuid(tenant_id)
    key = service_key if isinstance(service_key, str) else None
    for rule in rules:
        if _canonical_uuid(rule.subject_id) != canon_subject:
            continue
        # An unparseable tenant_id yields None, which can never equal a pinned
        # rule value — so a malformed tenant BILLS. That is the safe direction
        # and it is asserted by a test: the narrowing must not be satisfiable by
        # sending garbage.
        if rule.tenant_id is not None and _canonical_uuid(rule.tenant_id) != canon_tenant:
            continue
        if rule.service_key is not None and rule.service_key != key:
            continue
        if rule.endpoint is not None and rule.endpoint != endpoint:
            continue
        # ⛔ A CONJUNCT, NOT A KEY. This can only ever make a rule match FEWER
        # events; it is unreachable except for an identity already matched on
        # all four preceding fields. `is not True` rather than `!=` so a bool
        # (True == 1 in Python) cannot satisfy a rule pinning 1 token.
        if rule.input_tokens is not None and (
            not isinstance(input_tokens, int)
            or isinstance(input_tokens, bool)
            or input_tokens != rule.input_tokens
        ):
            continue
        return rule
    return None


def _canonical_uuid(value: Any) -> str | None:
    """``str(uuid.UUID(value))`` or None. Never raises.

    Accepts a ``uuid.UUID`` as well as a string because ``cost_flusher`` parses
    ``subject_id`` into a UUID object before this is called, while the allowlist
    and the tests hold strings.
    """
    if isinstance(value, uuid.UUID):
        return str(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        return str(uuid.UUID(value))
    except ValueError, AttributeError, TypeError:
        return None
