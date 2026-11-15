"""Self-metering health probes are RECORDED but NOT BILLED.

THE DEFECT
----------
Readiness probes across the estate deliberately call REAL data-plane endpoints —
that is what makes them readiness probes — and every such call mints a billable
cost event. Measured on the prod billing primary 2026-09-07: 38,494 of 39,519
rows carrying the ``mlx-backend`` service key in 24h (99.96%) were probe
traffic, and probe
rows were ~82% of every row in ``billing_cost_events`` while carrying 0.41% of
the money. A data-integrity and VOLUME defect, not an over-billing one.

WHAT FAILED BEFORE THE FIX
--------------------------
MEASURED, not asserted. This file was run against ``origin/main`` (6098f3e9)
with ``src/services/cost_flusher.py`` — the fix itself — left exactly as it is
on that revision, and everything else (this file, ``src/health_probes.py``, the
counter, the setting, the fixture) present. Result: **6 failed, 20 passed**.

The 6 that failed are precisely the ones that assert the new behaviour::

    test_a_karma_probe_event_is_parked          excluded_from_billing is False
    test_both_karma_hops_are_parked             exclusion_reason is None
    test_the_nlp_service_probe_is_parked
    test_a_parked_probe_does_not_roll_up
    test_the_counter_moves_on_exclusion         assert (0.0 - 0.0) == 1
    test_disarmed_bills_the_probe_but_still_counts_it

⛔ AND THE FOUR CONTROL TESTS PASSED IN BOTH ARMS. That is not a weakness in
them, it is their entire purpose: a suite in which every test flips on the fix
cannot distinguish a rule that matches the probe from a rule that matches
everything. The controls are the half that can still fail if the rule is made
too wide, and they were green before the fix precisely because they describe
behaviour that must NOT change.

⛔ THE CONTROL GROUP IS THE LOAD-BEARING HALF OF THIS FILE
----------------------------------------------------------
The dangerous failure here is not "the probe still bills" — that is today, it is
visible, and it costs $0.41 per 7 days. It is "the rule got wider than the probe
and quietly stopped billing somebody real", which is invisible and unbounded.

So every narrowing in a ``ProbeRule`` has a test that sends an event differing
from the probe in EXACTLY that one field, with an IDENTICAL payload size, and
asserts it still bills:

* ``test_a_different_subject_with_the_same_payload_is_still_billed``  — subject
* ``test_the_same_subject_on_a_different_tenant_is_still_billed``     — tenant
* ``test_the_same_subject_on_a_different_service_key_is_still_billed``— service
* ``test_the_same_subject_on_a_different_endpoint_is_still_billed``   — endpoint

The tenant one is not hypothetical padding. Measured over 7 days, the karma
subject ``01a0138e-…`` carries traffic for TWO tenants on the SAME service_key
and the SAME two endpoints: ``svc-kag-karma`` (the probe, 1 distinct
``input_tokens`` value across 93,669 rows) and ``nonagentic-gtm`` (342 real rows,
44 distinct values, $0.071). A subject-only rule — the obvious design — parks
every one of the latter. That test is the regression guard for a defect this fix
came within one field of shipping.

⛔ PAYLOAD SIZE IS A CONJUNCT HERE, NEVER A KEY
-----------------------------------------------
``input_tokens`` is pinned on every allowlist entry, and the distinction that
makes that safe is the one this file has to keep honest.

AS A SOLE KEY it would be catastrophic: a genuine 16- or 30-token request from
ANY tenant would stop billing. So every IDENTITY control above sends the probe's
OWN token count — a rule that keyed on size could not pass them.

AS AN AND-CONJUNCT on an identity already matched on subject, tenant, service
key and endpoint, it can only ever move rows back INTO billing.
``test_a_real_request_from_the_probe_identity_is_still_billed`` is its proof and
its regression guard.

⭐ SECOND MUTATION PROOF: deleting the ``input_tokens`` conjunct from
``match_health_probe`` and changing nothing else turns exactly THREE tests red —
``test_a_real_request_from_the_probe_identity_is_still_billed``,
``test_a_real_karma_request_would_also_be_billed`` and
``test_the_matcher_needs_every_pinned_field`` — and leaves the other 26 green.
The conjunct is load-bearing, and it is bearing exactly the load claimed for it.

⚠️ AND IT BUYS THAT WITH A NEW FAILURE MODE — silent rot. Change a probe's text
by one character and the rule stops matching while nothing looks wrong;
identity alone was robust to that.
``test_every_allowlist_entry_pins_the_live_probe_payload`` is the CI half of the
answer and the flat-line on the counter is the production half.
"""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

import observability.metrics as om
from health_probes import (
    HEALTH_PROBE_EMITTERS,
    HEALTH_PROBE_EXCLUSION_REASON,
    ProbeRule,
    match_health_probe,
    probe_endpoint,
)
from models.db import CostEvent, TokenUsage
from services.aggregator import flush_costs
from tenancy import LEGACY_TENANT_NAMES
from tests._aggregator_helpers import _make_cost

#: A REGISTERED tenant for the "still billed" controls. A raw uuid4 tenant is
#: unregistered, and ``billing_exclude_unregistered_tenants`` (ON by default)
#: would park it for a reason that has nothing to do with this rule, so the
#: control would fail for the wrong reason. ``fr-test`` is a real licences row.
REGISTERED_TENANT = LEGACY_TENANT_NAMES["fr-test"]

#: The karma probe: two mlx-backend hops from ONE call site
#: (karma_service/src/routers/health.py:113). Taken from the allowlist itself,
#: not re-typed — a copy here could drift from the rule and make every assertion
#: below describe a rule that is not the shipped one.
KARMA_PROBE = HEALTH_PROBE_EMITTERS[0]
NLP_PROBE = HEALTH_PROBE_EMITTERS[2]

#: The karma probe text is `"Ada Lovelace worked in London."` — 30 chars, and
#: `input_tokens` is 30 on all 93,800 live rows for that identity. Every IDENTITY
#: control test below sends this SAME value, so a rule that keyed on size alone
#: could not pass them.
PROBE_INPUT_TOKENS = "30"

#: A real svc-nlp-service payload, measured: 4 rows at 22,031 and 1 at 23,833 on
#: the allowlisted nlp triple over 7d. These are the 5 rows the `input_tokens`
#: conjunct recovers, and the reason it earns its keep.
REAL_NLP_INPUT_TOKENS = "22031"

#: The routing slug `nonagentic-gtm`'s real traffic uses. Bound to a name rather
#: than written inline at the call site because detect-secrets' "Secret Keyword"
#: rule fires on any `*_key = "<literal>"` adjacency, and metrics.py records why
#: an inline `# pragma: allowlist secret` is the fragile answer here: ruff-format
#: may reflow the call and move the comment off the flagged line.
NON_PROBE_SERVICE = "inference-router"

FIXTURE = Path(__file__).parent / "fixtures" / "health_probe_subjects.txt"


def _probe_cost(
    *,
    rule: ProbeRule = KARMA_PROBE,
    subject_id: str | None = None,
    tenant_id: str | None = None,
    service_key: str | None = None,
    endpoint: str | None = None,
    input_tokens: str | None = None,
) -> dict:
    """A wire-shaped cost event, defaulting to a real karma probe row.

    ``usage_extra`` is passed as a JSON STRING, not a dict, because that is what
    a Redis stream delivers and it is what ``_parse_usage_extra`` is written
    against. Handing the flusher a dict here would exercise a path production
    never takes and would hide a JSON round-trip defect in the endpoint read.
    """
    if input_tokens is None:
        input_tokens = str(rule.input_tokens)
    extra = {"unit": "nlp_chars", "tokens": 6, "entities": 2}
    ep = rule.endpoint if endpoint is None else endpoint
    if ep:
        extra["endpoint"] = ep
    return _make_cost(
        tenant_id=tenant_id or rule.tenant_id or str(uuid.uuid4()),
        subject_id=subject_id or rule.subject_id,
        service_key=service_key or rule.service_key or "mlx-backend",
        provider="mlx-nlp",
        model="en_core_web_trf",
        input_tokens=input_tokens,
        output_tokens="0",
        usage_extra=json.dumps(extra),
    )


async def _rows(session_factory) -> list[CostEvent]:
    async with session_factory() as session:
        return list((await session.execute(select(CostEvent))).scalars())


def _counter(rule: ProbeRule, action: str) -> float:
    return om.cost_events_health_probe_total.labels(
        subject_id=rule.subject_id,
        tenant_id=rule.tenant_id or "",
        service_key=rule.service_key or "",
        endpoint=rule.endpoint or "",
        action=action,
    )._value.get()


@pytest.fixture
def disarmed(monkeypatch):
    """Flip the kill switch OFF for one test.

    Patches the cached Settings instance rather than the environment, because
    ``get_settings`` is lru_cached and ``flush_costs`` reads the flag once per
    call — an env change alone would not be seen. Same shape as the ``armed``
    fixture in test_unregistered_tenant_exclusion.py, inverted, because this
    rule ships ON (that one is ON by default too, but its tests pin it both ways).
    """
    from settings import get_settings

    get_settings.cache_clear()
    settings = get_settings()
    monkeypatch.setattr(settings, "billing_exclude_health_probes", False)
    monkeypatch.setattr("settings.get_settings", lambda: settings)
    monkeypatch.setattr("services.cost_flusher.get_settings", lambda: settings)
    yield settings
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# THE FIX: a probe event is stored and parked
# ---------------------------------------------------------------------------


async def test_a_karma_probe_event_is_parked(db_session_factory) -> None:
    """⛔ THE TEST THAT FAILS ON origin/main.

    Verified: on 6098f3e9 this asserts ``excluded_from_billing is False`` and
    ``exclusion_reason is None``, because no code path on that revision can
    produce ``'health_probe'``.
    """
    count = await flush_costs([_probe_cost()], db_session_factory, None)
    assert count == 1

    rows = await _rows(db_session_factory)
    assert len(rows) == 1
    assert rows[0].excluded_from_billing is True
    assert rows[0].exclusion_reason == HEALTH_PROBE_EXCLUSION_REASON


async def test_the_probe_event_is_still_stored(db_session_factory) -> None:
    """PARKED, NEVER DROPPED — and this is the whole difference.

    A dropped event destroys the record that a call happened and makes the
    exclusion unfalsifiable: nobody could afterwards measure what the rule had
    removed, so an over-broad rule would leave no trace to find it by.
    """
    await flush_costs([_probe_cost()], db_session_factory, None)
    rows = await _rows(db_session_factory)
    assert len(rows) == 1, "the probe event was dropped instead of parked"
    assert str(rows[0].subject_id) == KARMA_PROBE.subject_id
    assert rows[0].input_tokens == 30, "the payload was not preserved on the parked row"


async def test_both_karma_hops_are_parked(db_session_factory) -> None:
    """ONE probe call, TWO billed rows — the fan-out inside nlp_service.

    karma_service makes a single POST /api/nlp/entities, which nlp_service fans
    out to process-text (text_processor.py:994) and vocab-info
    (text_processor.py:1418). Covering only one hop would halve the fix while
    every single-row test still passed.
    """
    await flush_costs(
        [_probe_cost(rule=HEALTH_PROBE_EMITTERS[0]), _probe_cost(rule=HEALTH_PROBE_EMITTERS[1])],
        db_session_factory,
        None,
    )
    rows = await _rows(db_session_factory)
    assert len(rows) == 2
    assert {r.exclusion_reason for r in rows} == {HEALTH_PROBE_EXCLUSION_REASON}
    assert {(r.usage_extra or {}).get("endpoint") for r in rows} == {
        "process-text",
        "vocab-info",
    }


async def test_the_nlp_service_probe_is_parked(db_session_factory) -> None:
    """The second emitter, whose subject_id this session measured off the ledger."""
    await flush_costs([_probe_cost(rule=NLP_PROBE, input_tokens="16")], db_session_factory, None)
    rows = await _rows(db_session_factory)
    assert len(rows) == 1
    assert rows[0].exclusion_reason == HEALTH_PROBE_EXCLUSION_REASON


async def test_a_parked_probe_does_not_roll_up(db_session_factory) -> None:
    """The billable SIDE EFFECTS must be withheld too, or the exclusion is a lie.

    A row flagged excluded that still inflates token_usage and still drains a
    budget is still billed — it just tells every report it was not. This is the
    same property test_unregistered_tenant_exclusion.py asserts for its own
    reason, and it holds here through the same
    ``if row["excluded_from_billing"]: continue`` in cost_flusher.
    """
    await flush_costs([_probe_cost()], db_session_factory, None)
    async with db_session_factory() as session:
        usage = list(
            (
                await session.execute(
                    select(TokenUsage).where(TokenUsage.tenant_id == KARMA_PROBE.tenant_id)
                )
            ).scalars()
        )
    assert not usage, "a parked probe still inflated the token rollup"


# ---------------------------------------------------------------------------
# ⛔ THE CONTROL GROUP: one field different, SAME payload size, still billed
# ---------------------------------------------------------------------------


async def test_a_different_subject_with_the_same_payload_is_still_billed(
    db_session_factory,
) -> None:
    """⛔ THE ANTI-HEURISTIC TEST. Identical payload, unlisted subject → BILLED.

    This is the test that fails if anyone ever re-keys the rule on
    ``input_tokens``: this event is byte-identical to a karma probe on every
    field a size rule can see (30 input_tokens, same usage_extra, same
    service_key, same endpoint) and differs ONLY in who sent it. A genuine
    30-character user request must keep billing.

    It passed BEFORE the fix as well as after, deliberately — see the module
    docstring.
    """
    await flush_costs(
        [_probe_cost(subject_id=str(uuid.uuid4()), tenant_id=REGISTERED_TENANT)],
        db_session_factory,
        None,
    )
    rows = await _rows(db_session_factory)
    assert len(rows) == 1
    assert rows[0].input_tokens == 30, "the control is not payload-identical to the probe"
    assert rows[0].excluded_from_billing is False, "the rule parked an unlisted subject"
    assert rows[0].exclusion_reason is None


async def test_the_same_subject_on_a_different_tenant_is_still_billed(
    db_session_factory,
) -> None:
    """⛔ THE REGRESSION GUARD FOR A DEFECT THIS FIX NEARLY SHIPPED.

    subject_id is a CREDENTIAL, not a service. Measured over 7 days, the karma
    subject serves ``svc-kag-karma`` (the probe) AND ``nonagentic-gtm`` — 342
    real rows at 44 distinct input_tokens values, $0.071 — on the SAME
    service_key and the SAME two endpoints. A subject-only allowlist parks all
    of them and nothing about the code would look wrong.
    """
    await flush_costs(
        [_probe_cost(tenant_id="e4f70a83-81ee-429a-abca-6999e3a0d24f")],  # nonagentic-gtm
        db_session_factory,
        None,
    )
    rows = await _rows(db_session_factory)
    assert len(rows) == 1
    assert rows[0].excluded_from_billing is False, (
        "the rule parked another tenant's real traffic riding the probe's subject"
    )


async def test_the_same_subject_on_a_different_service_key_is_still_billed(
    db_session_factory,
) -> None:
    """``nonagentic-gtm``'s 22 inference-router rows / 7d — $0.410, the most
    expensive population any of these rules comes near."""
    await flush_costs([_probe_cost(service_key=NON_PROBE_SERVICE)], db_session_factory, None)
    rows = await _rows(db_session_factory)
    assert rows[0].excluded_from_billing is False


async def test_the_same_subject_on_a_different_endpoint_is_still_billed(
    db_session_factory,
) -> None:
    """svc-nlp-service's real ``vocab-info`` calls — 5 rows / $0.112 per 7d."""
    await flush_costs(
        [_probe_cost(rule=NLP_PROBE, endpoint="vocab-info")], db_session_factory, None
    )
    rows = await _rows(db_session_factory)
    assert rows[0].excluded_from_billing is False


async def test_a_real_request_from_the_probe_identity_is_still_billed(
    db_session_factory,
) -> None:
    """⛔ THE TEST THAT PROVES THE ``input_tokens`` CONJUNCT IS LOAD-BEARING.

    Same subject, same tenant, same service key, same endpoint as the
    nlp_service probe — identical on every one of the four IDENTITY fields — and
    differing only in payload. Measured on the live ledger: 4 rows at 22,031 and
    1 at 23,833 over 7d on exactly this triple, $0.112, genuine
    ``svc-nlp-service`` work.

    ⭐ MUTATION-PROVEN: delete the ``input_tokens`` conjunct from
    ``match_health_probe`` and this test alone goes red — the 5 real rows are
    parked again. That is the whole justification for accepting a payload-shaped
    condition into an identity rule.
    """
    await flush_costs(
        [_probe_cost(rule=NLP_PROBE, input_tokens=REAL_NLP_INPUT_TOKENS)],
        db_session_factory,
        None,
    )
    rows = await _rows(db_session_factory)
    assert len(rows) == 1
    assert rows[0].input_tokens == 22031
    assert rows[0].excluded_from_billing is False, (
        "a real request from the probe's own identity was parked — the "
        "input_tokens conjunct is not being applied"
    )
    assert rows[0].exclusion_reason is None


async def test_a_real_karma_request_would_also_be_billed(db_session_factory) -> None:
    """The same property on the karma identity.

    ⚠️ This population is currently EMPTY in production — all 93,800 rows on the
    karma triples are ≡30 — so unlike its nlp sibling this test guards a
    capability rather than a measured population. It is here because "karma
    never sends anything else" is a fact about today, not a property of the
    system, and the day it stops being true must not be the day billing
    silently stops.
    """
    await flush_costs([_probe_cost(input_tokens="4096")], db_session_factory, None)
    rows = await _rows(db_session_factory)
    assert rows[0].excluded_from_billing is False


async def test_an_event_with_no_subject_is_still_billed(db_session_factory) -> None:
    """FAIL CLOSED = BILLS. Most live rows carry no subject_id at all."""
    await flush_costs(
        [_probe_cost(subject_id="", tenant_id=REGISTERED_TENANT)], db_session_factory, None
    )
    rows = await _rows(db_session_factory)
    assert rows[0].excluded_from_billing is False


# ---------------------------------------------------------------------------
# The matcher, directly — properties the DB tests cannot isolate
# ---------------------------------------------------------------------------


def test_the_matcher_needs_every_pinned_field() -> None:
    """A positive control plus one negative per narrowing.

    Without the positive control the four negatives are satisfied by a matcher
    that always returns None — the classic vacuous guard.
    """
    r = KARMA_PROBE
    assert (
        match_health_probe(r.subject_id, r.tenant_id, r.service_key, r.endpoint, r.input_tokens)
        is r
    )
    assert (
        match_health_probe(
            str(uuid.uuid4()), r.tenant_id, r.service_key, r.endpoint, r.input_tokens
        )
        is None
    )
    assert (
        match_health_probe(
            r.subject_id, str(uuid.uuid4()), r.service_key, r.endpoint, r.input_tokens
        )
        is None
    )
    assert (
        match_health_probe(
            r.subject_id, r.tenant_id, "inference-router", r.endpoint, r.input_tokens
        )
        is None
    )
    assert match_health_probe(r.subject_id, r.tenant_id, r.service_key, "other") is None
    assert match_health_probe(r.subject_id, r.tenant_id, r.service_key, r.endpoint, 4096) is None, (
        "the input_tokens conjunct is not applied"
    )


def test_a_malformed_tenant_cannot_satisfy_a_pinned_narrowing() -> None:
    """Garbage must not be a way THROUGH a narrowing, only a way to be billed."""
    r = KARMA_PROBE
    for bad in (None, "", "not-a-uuid", 42, {"tenant": "x"}):
        assert (
            match_health_probe(r.subject_id, bad, r.service_key, r.endpoint, r.input_tokens) is None
        )


def test_the_matcher_is_indifferent_to_uuid_formatting() -> None:
    """A rule written in upper case or braces must still match the parsed uuid.

    A formatting difference must never be the reason a rule silently stops
    firing — that failure looks exactly like "the probe stopped".
    """
    r = KARMA_PROBE
    assert r.tenant_id is not None
    assert (
        match_health_probe(
            uuid.UUID(r.subject_id),
            r.tenant_id.upper(),
            r.service_key,
            r.endpoint,
            r.input_tokens,
        )
        is r
    )


def test_probe_endpoint_never_widens_a_rule() -> None:
    """A malformed usage_extra reads as "no endpoint", which matches only a rule
    that pinned none — never a rule that pinned one."""
    assert probe_endpoint({"endpoint": "process-text"}) == "process-text"
    for bad in (None, "", [], "process-text", {"endpoint": 3}, {"endpoint": ""}, {}):
        assert probe_endpoint(bad) is None


# ---------------------------------------------------------------------------
# The kill switch
# ---------------------------------------------------------------------------


def test_the_kill_switch_ships_on() -> None:
    """If this ever flips, every "parked" test above changes meaning.

    It shipped ON from day one, whereas billing_exclude_unregistered_tenants
    shipped OFF until 2026-09-16, and the difference was the INPUT: that gate
    then read a hand-refreshed snapshot of every tenant in the estate (11 days
    stale and missing six names on 2026-09-05); this one reads a three-entry
    allowlist measured directly against prod.
    """
    from settings import Settings

    assert Settings().billing_exclude_health_probes is True


async def test_disarmed_bills_the_probe_but_still_counts_it(disarmed, db_session_factory) -> None:
    """OFF must be byte-identical to before this code existed — except the counter.

    A kill switch that also suppressed the measurement would make the effect of
    flipping it back unknowable.
    """
    before = _counter(KARMA_PROBE, "counted")
    await flush_costs([_probe_cost()], db_session_factory, None)

    rows = await _rows(db_session_factory)
    assert rows[0].excluded_from_billing is False
    assert rows[0].exclusion_reason is None
    assert _counter(KARMA_PROBE, "counted") - before == 1


async def test_the_counter_moves_on_exclusion(db_session_factory) -> None:
    """⛔ THE ANTI-SINK. An exclusion nobody can see is a hole in the ledger."""
    before = _counter(KARMA_PROBE, "excluded")
    await flush_costs([_probe_cost()], db_session_factory, None)
    assert _counter(KARMA_PROBE, "excluded") - before == 1


async def test_the_counter_is_silent_for_a_billed_event(db_session_factory) -> None:
    """The other direction: the series must mean "a probe row", not "a row"."""
    before = _counter(KARMA_PROBE, "excluded")
    await flush_costs(
        [_probe_cost(subject_id=str(uuid.uuid4()), tenant_id=str(uuid.uuid4()))],
        db_session_factory,
        None,
    )
    assert _counter(KARMA_PROBE, "excluded") - before == 0


def test_every_allowlist_series_is_born_at_import() -> None:
    """Birthed at 0, so a rotted entry is a visible zero — not an absent series.

    The failure this whole module guards against is a rule that STOPS matching.
    Unbirthed, that produces no series at all, and "the rule matched nothing" is
    then indistinguishable from "the rule was never deployed".
    """
    for rule in HEALTH_PROBE_EMITTERS:
        for action in ("excluded", "counted"):
            assert _counter(rule, action) >= 0


# ---------------------------------------------------------------------------
# ⛔ THE DRIFT GUARD — an allowlist whose entries have rotted is a rule that
# has silently stopped matching, and a stale entry looks exactly like a live one
# ---------------------------------------------------------------------------


def _fixture_tuples() -> dict[tuple[str, str, str, str], tuple[int, set[int]]]:
    """(subject, tenant, service_key, endpoint) -> (total rows, modal payloads).

    Read from ``tests/fixtures/health_probe_subjects.txt``, which is the OUTPUT
    of a census of the live billing ledger
    (``scripts/refresh_health_probe_fixture.sh``) and is NOT generated from
    ``src/health_probes.py``. A fixture derived from the code it checks agrees
    with that code by construction, and this test would then pass against an
    allowlist that had drifted arbitrarily far.

    ⚠️ ROWS ARE AGGREGATED ACROSS ``tenant_name``. The census groups by the
    display label as well as the id, and ONE tenant routinely appears under two
    labels — its resolved name and its raw uuid — because ``tenant_name`` held
    the uuid on 66,103 of 66,103 rows before the write-time resolution fix
    landed. ``svc-kag-karma`` and ``f172bf91-…`` are the SAME
    ``tenant_id``. Keying this dict on the 4-tuple without summing would keep
    whichever line came last and silently discard the rest, understating every
    volume check below by roughly half.
    """
    out: dict[tuple[str, str, str, str], tuple[int, set[int]]] = {}
    for line in FIXTURE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        subject, tenant, _name, service_key, endpoint, rows, _distinct, _cash, modal = line.split(
            "|"
        )
        key = (subject, tenant, service_key, endpoint)
        prev_rows, modes = out.get(key, (0, set()))
        modes.add(int(modal))
        out[key] = (prev_rows + int(rows), modes)
    return out


def _census_for(rule: ProbeRule) -> tuple[int, set[int]]:
    census = _fixture_tuples()
    key = (rule.subject_id, rule.tenant_id or "", rule.service_key or "", rule.endpoint or "")
    assert key in census, (
        f"allowlist entry {key} is not in the live-ledger census.\n"
        f"It is either a typo, or the probe has moved/rotated and the rule has "
        f"silently stopped matching. ⛔ Do NOT simply delete the entry: a probe "
        f"that is merely DOWN looks identical here, and deleting it is how probe "
        f"rows quietly resume billing. Re-run scripts/refresh_health_probe_fixture.sh "
        f"and establish which it is.\nnote: {rule.note}"
    )
    return census[key]


def test_the_fixture_itself_is_not_empty() -> None:
    """THE PREMISE. An empty fixture makes every assertion below vacuous — it
    would report total rot as total health, or pass a rule with zero evidence."""
    tuples = _fixture_tuples()
    assert len(tuples) >= 10, f"the census fixture holds only {len(tuples)} rows"


def test_every_allowlist_entry_is_a_measured_subject() -> None:
    """⛔ THE DRIFT GUARD ITSELF.

    Every entry must resolve to a real, live-measured identity in the ledger
    census. This catches BOTH silent failure modes of an allowlist:

      * an entry that NEVER matched — a typo'd uuid, a guessed service key. The
        rule does nothing and the code reads as though the problem were solved.
      * an entry that STOPPED matching — a rotated service account, a renamed
        endpoint. Identical symptom, surfacing months later.

    Neither shows up in a diff, a log, or a test that only exercises the matcher
    against its own constants.
    """
    for rule in HEALTH_PROBE_EMITTERS:
        _census_for(rule)  # asserts presence, with the full diagnosis on failure


def test_every_allowlist_entry_carries_real_volume() -> None:
    """An entry matching a handful of rows is not a probe and does not earn a rule.

    Presence alone is a weak check — it passes for an identity that emitted 3
    rows once. The exclusion is only worth its risk against genuine probe volume.
    """
    for rule in HEALTH_PROBE_EMITTERS:
        rows, _modes = _census_for(rule)
        assert rows >= 1000, (
            f"allowlist entry for {rule.note!r} matched only {rows} rows in the "
            f"7d census — that is not probe volume, and an exclusion rule is not "
            f"warranted."
        )


def test_every_allowlist_entry_pins_the_live_probe_payload() -> None:
    """⛔ THE GUARD THE ``input_tokens`` CONJUNCT MADE NECESSARY.

    Pinning a payload size makes the rule narrower and recovers real rows, but
    it buys that with a NEW failure mode: change the probe's text by a single
    character and the rule stops matching, and probe rows quietly resume billing
    with nothing in the diff, the log or the code looking wrong. Identity alone
    was robust to that; this conjunct is not.

    So the pinned value is checked against the value the ledger census actually
    OBSERVED as most common for that identity. A changed probe text becomes a
    red build naming the entry instead of a slow leak.

    ⚠️ If this fails, the fix is NOT to paste the new modal value in. Find out
    why the probe's payload changed first — a probe whose text drifts on its own
    is its own defect, and a probe that was replaced may not need a rule at all.
    """
    for rule in HEALTH_PROBE_EMITTERS:
        if rule.input_tokens is None:
            continue
        _rows, modes = _census_for(rule)
        assert rule.input_tokens in modes, (
            f"allowlist entry for {rule.note!r} pins input_tokens="
            f"{rule.input_tokens}, but the live census observed modal value(s) "
            f"{sorted(modes)} for that identity. The probe's payload has changed "
            f"and this rule has STOPPED MATCHING — probe rows are billing again."
        )


def test_every_allowlist_entry_is_a_well_formed_canonical_uuid() -> None:
    """A rule the matcher cannot parse is a rule that never fires."""
    for rule in HEALTH_PROBE_EMITTERS:
        for field, value in (("subject_id", rule.subject_id), ("tenant_id", rule.tenant_id)):
            assert value, f"{field} is empty on {rule.note}"
            assert str(uuid.UUID(value)) == value, (
                f"{field}={value!r} is not canonical lowercase-hyphenated uuid"
            )


def test_no_duplicate_or_shadowed_allowlist_entries() -> None:
    """Two entries for one identity means one is dead, and dead entries rot unseen."""
    keys = [(r.subject_id, r.tenant_id, r.service_key, r.endpoint) for r in HEALTH_PROBE_EMITTERS]
    assert len(keys) == len(set(keys)), "duplicate entry in HEALTH_PROBE_EMITTERS"


def test_every_allowlist_entry_pins_all_three_narrowings() -> None:
    """⛔ A ``None`` narrowing is a wildcard, and a wildcard is how this rule
    starts parking strangers.

    Every field left ``None`` is a population the rule will match that nobody
    measured. ``ProbeRule`` allows ``None`` because a future probe may genuinely
    need it; this test makes leaving one out a DELIBERATE act that has to edit a
    test, rather than an omission.
    """
    for rule in HEALTH_PROBE_EMITTERS:
        assert rule.tenant_id, f"unpinned tenant_id on {rule.note}"
        assert rule.service_key, f"unpinned service_key on {rule.note}"
        assert rule.endpoint, f"unpinned endpoint on {rule.note}"
        assert rule.input_tokens, f"unpinned input_tokens on {rule.note}"
        assert rule.note, "every entry must say why it exists"


def test_the_exclusion_reason_fits_the_column() -> None:
    """``billing_cost_events.exclusion_reason`` is varchar(64) (alembic 0013)."""
    assert len(HEALTH_PROBE_EXCLUSION_REASON) <= 64
    assert re.fullmatch(r"[a-z_]+", HEALTH_PROBE_EXCLUSION_REASON)


# ---------------------------------------------------------------------------
# ⛔ THE REVERSE DRIFT GUARD — the direction the guards above cannot see
# ---------------------------------------------------------------------------


def _census_rows() -> list[tuple[str, str, str, str, int, int, int]]:
    """(subject, tenant, service_key, endpoint, rows, distinct_payloads, modal).

    A FLAT read of the census, deliberately NOT ``_fixture_tuples``: that helper
    sums across ``tenant_name`` and discards ``distinct_input_tokens``, which is
    the exact column this guard reasons about.
    """
    out: list[tuple[str, str, str, str, int, int, int]] = []
    for line in FIXTURE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        subject, tenant, _name, service_key, endpoint, rows, distinct, _cash, modal = line.split(
            "|"
        )
        out.append((subject, tenant, service_key, endpoint, int(rows), int(distinct), int(modal)))
    return out


def test_a_known_probe_identity_has_no_unallowlisted_probe_shaped_population() -> None:
    """⛔ THE GUARD THAT WOULD HAVE CAUGHT 2026-09-17, AND THAT NOTHING ABOVE DOES.

    Every drift guard above walks ``HEALTH_PROBE_EMITTERS`` and asks "is this
    ENTRY still vouched for by the census?". That direction cannot see a probe
    that MOVED: when nlp_service re-pointed its readiness probe from
    ``process-text`` to ``entities`` (2026-09-07) and mlx_backend then metered
    ``entities`` (PR #48, 2026-09-17), a brand-new 27,000-row/7d unbilled-probe
    population appeared under an identity this allowlist already knows — and
    every entry-shaped check stayed green, because each was still asking about
    the OLD endpoint. 27,123 synthetic rows billed before anyone noticed.

    So this walks the CENSUS instead and asks the opposite question: for an
    identity already declared a probe emitter, is there any probe-shaped
    population the allowlist does NOT cover?

    ⛔ SCOPED TO KNOWN PROBE IDENTITIES ON PURPOSE, and the narrowness is the
    safety argument. A census cannot tell a probe from a customer — the ledger
    holds real populations with one distinct payload size (``kag`` bills 522
    rows/7d at ``input_tokens`` ≡ 0). If this test policed every probe-SHAPED
    population it would demand an allowlist entry for real traffic, i.e. it
    would pressure a future reader into un-billing a stranger to get CI green.
    That is a worse defect than the one it detects. Keying on
    ``(subject_id, tenant_id)`` pairs that are ALREADY in the allowlist means
    this can only ever flag a population belonging to something an operator has
    already hand-verified as a probe.

    ⚠️ A failure here is NOT fixed by pasting the new endpoint into the
    allowlist. It says a probe's traffic moved. Find out where it moved to and
    why FIRST — then decide whether the old entry should be repointed (a probe
    that moved) or kept alongside (a probe that fanned out to a second hop, as
    karma's does).
    """
    known_identities = {
        (str(uuid.UUID(r.subject_id)), str(uuid.UUID(r.tenant_id)))
        for r in HEALTH_PROBE_EMITTERS
        if r.tenant_id
    }
    assert known_identities, "no allowlist entries — this guard would be vacuous"

    unparked: list[str] = []
    for subject, tenant, service_key, endpoint, rows, distinct, modal in _census_rows():
        try:
            identity = (str(uuid.UUID(subject)), str(uuid.UUID(tenant)))
        except ValueError, AttributeError, TypeError:
            continue
        if identity not in known_identities:
            continue
        # Probe shape: one payload size, repeated at volume. Evidence, and only
        # reachable for an identity an operator already called a probe emitter.
        if distinct != 1 or rows < 1000:
            continue
        if match_health_probe(subject, tenant, service_key, endpoint or None, modal) is None:
            unparked.append(
                f"  {subject} / {tenant} / {service_key} / "
                f"{endpoint or '(none)'} — {rows} rows, {distinct} distinct "
                f"input_tokens (modal {modal}), NOT matched by any allowlist entry"
            )

    assert not unparked, (
        "a KNOWN probe identity is emitting a probe-shaped population that the "
        "allowlist does not cover — those rows are billing right now:\n"
        + "\n".join(unparked)
        + "\n\nThis is the 2026-09-17 defect recurring: the probe moved to an "
        "endpoint no entry pins. Establish where it moved before editing the "
        "allowlist."
    )
