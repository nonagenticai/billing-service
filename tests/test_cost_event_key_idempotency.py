"""The producer-minted `event_key` claim — exactly-once across a re-emit.

`ProcessedCostMessage` dedupes on the Redis MESSAGE id, which Redis mints at
XADD time. It therefore cannot see a producer that re-XADDs the same physical
call (a retried emit, a replayed outbox, a stream recreated under the producer):
those are new message ids carrying a call billing has already charged for.

worker_management now stamps `event_key`, a uuid7 minted once per completed LLM
call. These tests are the enforcement half of that contract.
"""

import uuid

import pytest
from sqlalchemy import func, select

from models.db import CostEvent, ProcessedCostEventKey
from services.aggregator import flush_costs
from tests._aggregator_helpers import _make_cost
from tests.conftest import T

pytestmark = pytest.mark.unit


def _cost(tenant: str, key: str | None, **over):
    row = _make_cost(tenant_id=tenant, license_id=str(uuid.uuid4()), **over)
    if key is not None:
        row["event_key"] = key
    return row


async def _count(session, tenant: str) -> int:
    return (
        await session.execute(
            select(func.count()).select_from(CostEvent).where(CostEvent.tenant_id == tenant)
        )
    ).scalar()


async def test_a_replayed_emit_is_not_rebilled(db_session_factory):
    """Same event_key, two separate flushes ⇒ one billing row."""
    tenant = T("replay-tenant")
    key = str(uuid.uuid7())

    assert await flush_costs([_cost(tenant, key)], db_session_factory, None) == 1
    # A different Redis message id — the message-id inbox cannot catch this one.
    await flush_costs([_cost(tenant, key)], db_session_factory, None)

    async with db_session_factory() as session:
        assert await _count(session, tenant) == 1


async def test_a_replay_inside_one_batch_is_collapsed(db_session_factory):
    """pg_insert would ABORT on a duplicate VALUES row rather than dedupe it."""
    tenant = T("replay-batch-tenant")
    key = str(uuid.uuid7())

    await flush_costs([_cost(tenant, key), _cost(tenant, key)], db_session_factory, None)

    async with db_session_factory() as session:
        assert await _count(session, tenant) == 1


async def test_two_distinct_calls_are_both_billed(db_session_factory):
    """The regression guard: the claim must not swallow real spend."""
    tenant = T("distinct-tenant")

    await flush_costs(
        [_cost(tenant, str(uuid.uuid7())), _cost(tenant, str(uuid.uuid7()))],
        db_session_factory,
        None,
    )

    async with db_session_factory() as session:
        assert await _count(session, tenant) == 2


async def test_a_keyless_producer_is_not_rejected(db_session_factory):
    """An absent key means "cannot promise uniqueness", never "drop this event".

    Every producer other than worker_management still emits without a key, and
    silently dropping their spend would be a far worse defect than the one this
    key exists to close.
    """
    tenant = T("keyless-tenant")

    await flush_costs([_cost(tenant, None), _cost(tenant, None)], db_session_factory, None)

    async with db_session_factory() as session:
        assert await _count(session, tenant) == 2


async def test_the_key_is_persisted_on_the_event_and_claimed(db_session_factory):
    tenant = T("persist-tenant")
    key = str(uuid.uuid7())

    await flush_costs([_cost(tenant, key)], db_session_factory, None)

    async with db_session_factory() as session:
        row = (
            (await session.execute(select(CostEvent).where(CostEvent.tenant_id == tenant)))
            .scalars()
            .one()
        )
        assert row.event_key == key
        claimed = (
            await session.execute(
                select(func.count())
                .select_from(ProcessedCostEventKey)
                .where(ProcessedCostEventKey.event_key == key)
            )
        ).scalar()
        assert claimed == 1


# ---------------------------------------------------------------------------
# Derivation at the accept seam
#
# Multi-valued ON PURPOSE: every case below exercises TWO different producers
# with TWO different key shapes (a uuid7 the producer minted vs a `d1:` digest
# billing derived from a provider_request_id). A mutant that hard-codes one
# shape, or that agrees with one producer's convention, cannot survive both.
# ---------------------------------------------------------------------------


def test_the_derivation_is_grounded_in_the_provider_id_not_the_content():
    """⛔ THE CENTRAL REGRESSION GUARD — read the docstring on `derive_event_key`.

    Two byte-identical calls that differ ONLY in provider_request_id must get
    DIFFERENT keys. 26,283 live mlx-backend rows share one exact content
    signature; a content-hash derivation would collapse them and invent
    catastrophic under-billing. This test fails the moment anyone folds the
    tokens/model/cost into the material.
    """
    from services._parsers import derive_event_key

    a = {
        "tenant_id": "t1",
        "service_key": "inference-router",
        "provider_request_id": "req-a",
        "model": "m",
        "input_tokens": "30",
    }
    b = dict(a, provider_request_id="req-b")

    ka, sa = derive_event_key(a)
    kb, sb = derive_event_key(b)

    assert sa == sb == "provider_request_id"
    assert ka != kb, "identical content must not collapse to one key"
    # ...and the SAME provider id is stable across a re-emit, which is the whole
    # point: this is the half that actually dedupes.
    assert derive_event_key(dict(a))[0] == ka


def test_two_producers_two_key_shapes_are_both_honoured():
    """A producer-stamped uuid7 survives verbatim; a keyless one gets a `d1:` key."""
    from services._parsers import derive_event_key

    stamped = str(uuid.uuid7())
    k1, s1 = derive_event_key({"tenant_id": "t", "service_key": "w2v-tts", "event_key": stamped})
    k2, s2 = derive_event_key(
        {"tenant_id": "t", "service_key": "inference-router", "provider_request_id": "req-9"}
    )

    assert (k1, s1) == (stamped, "producer")
    assert k1 is not None and k2 is not None
    assert s2 == "provider_request_id" and k2.startswith("d1:")
    # Two shapes, one column: both must fit VARCHAR(64) or the INSERT dies.
    assert len(k1) <= 64 and len(k2) <= 64
    # A derived key can never be mistaken for a producer's.
    assert not k1.startswith("d1:")


def test_a_producer_with_no_identity_at_all_stays_absent_and_is_not_rejected():
    """82.6% of live rows are this case (mlx-backend, kag). NULL is the honest
    answer — a minted-per-emit uuid would dedupe nothing while satisfying a
    NOT NULL check, and a rejection here would refuse five sixths of traffic."""
    from services._parsers import derive_event_key

    for svc in ("mlx-backend", "kag"):
        # Both spellings of "nothing usable": absent, and the empty string the
        # live mlx-backend rows actually carry in provider_request_id.
        assert derive_event_key({"tenant_id": "t", "service_key": svc}) == (None, "absent")
        assert derive_event_key(
            {"tenant_id": "t", "service_key": svc, "provider_request_id": "", "event_key": ""}
        ) == (None, "absent")
    # A sentinel is absence too, not an identity to key on.
    assert derive_event_key(
        {"tenant_id": "t", "service_key": "kag", "provider_request_id": "unknown"}
    ) == (None, "absent")


def test_the_derived_key_is_tenant_scoped():
    """The same provider_request_id under two tenants must not cross-dedupe —
    that would drop one tenant's billable call because another tenant's provider
    happened to reuse an id."""
    from services._parsers import derive_event_key

    base = {"service_key": "inference-router", "provider_request_id": "req-shared"}
    assert (
        derive_event_key(dict(base, tenant_id="t1"))[0]
        != derive_event_key(dict(base, tenant_id="t2"))[0]
    )


async def test_a_keyless_producer_with_a_provider_id_is_now_deduped(db_session_factory):
    """END TO END, and the behaviour change this PR ships: a producer that never
    stamps an event_key but DOES carry a provider_request_id is re-emitted, and
    is billed once instead of twice."""
    tenant = T("derived-dedup-tenant")
    row = _make_cost(
        tenant_id=tenant, license_id=str(uuid.uuid4()), provider_request_id="prov-req-42"
    )

    await flush_costs([dict(row)], db_session_factory, None)
    await flush_costs([dict(row)], db_session_factory, None)  # new msg id, same call

    async with db_session_factory() as session:
        assert await _count(session, tenant) == 1
        key = (
            await session.execute(select(CostEvent.event_key).where(CostEvent.tenant_id == tenant))
        ).scalar()
        assert key is not None and key.startswith("d1:")
        assert (
            await session.execute(
                select(func.count())
                .select_from(ProcessedCostEventKey)
                .where(ProcessedCostEventKey.event_key == key)
            )
        ).scalar() == 1


async def test_two_distinct_keyless_calls_are_still_both_billed(db_session_factory):
    """The other direction — the derivation must not swallow real spend."""
    tenant = T("derived-distinct-tenant")
    lic = str(uuid.uuid4())
    await flush_costs(
        [
            _make_cost(tenant_id=tenant, license_id=lic, provider_request_id="prov-a"),
            _make_cost(tenant_id=tenant, license_id=lic, provider_request_id="prov-b"),
        ],
        db_session_factory,
        None,
    )
    async with db_session_factory() as session:
        assert await _count(session, tenant) == 2


async def test_a_producer_with_no_identity_is_billed_every_time(db_session_factory):
    """mlx-backend's live shape: identical rows, no identity anywhere. These are
    26k distinct NLP calls, not duplicates — both MUST bill."""
    tenant = T("no-identity-tenant")
    lic = str(uuid.uuid4())
    row = dict(
        _make_cost(
            tenant_id=tenant,
            license_id=lic,
            service_key="mlx-backend",
            provider="mlx-nlp",
            model="en_core_web_trf",
            input_tokens="30",
            output_tokens="0",
        )
    )
    await flush_costs([dict(row), dict(row)], db_session_factory, None)
    async with db_session_factory() as session:
        assert await _count(session, tenant) == 2


# ---------------------------------------------------------------------------
# The two ways rung 1 could silently MERGE two distinct billable calls.
#
# Both are the same defect the content-hash rejection in `derive_event_key`'s
# docstring is about — a collision that collapses distinct calls into one key
# and under-bills — arriving by a different route. Both are multi-valued: each
# asserts two DIFFERENT inputs stay apart, so a mutant that hard-codes any one
# value cannot survive.
# ---------------------------------------------------------------------------


def test_a_placeholder_event_key_is_not_an_identity_on_rung_1():
    """A sentinel on the wire is absence, and rung 1 must read it the way rung 2
    does. Honouring `event_key="unknown"` verbatim would collapse EVERY such row
    onto one key and drop all but the first — under-billing, silently.

    Multi-valued twice over: five sentinel spellings across two producers, each
    carrying a DISTINCT provider_request_id, so the correct behaviour is five
    distinct derived keys rather than one shared literal.
    """
    from services._parsers import derive_event_key

    sentinels = ["unknown", "N/A", "  None  ", "-", "UNDEFINED"]
    keys = []
    for i, sentinel in enumerate(sentinels):
        svc = "inference-router" if i % 2 else "w2v-tts"
        key, source = derive_event_key(
            {
                "tenant_id": "t",
                "service_key": svc,
                "event_key": sentinel,
                "provider_request_id": f"req-{i}",
            }
        )
        assert source == "provider_request_id", f"{sentinel!r} was honoured as an identity"
        assert key is not None
        assert key not in sentinels and key.startswith("d1:")
        keys.append(key)

    assert len(set(keys)) == len(sentinels), "placeholder keys collapsed onto one key"

    # With nothing else on the wire, a sentinel key is absence — not an identity.
    for sentinel in sentinels:
        assert derive_event_key(
            {"tenant_id": "t", "service_key": "kag", "event_key": sentinel}
        ) == (None, "absent")


def test_two_over_long_keys_sharing_a_64_char_prefix_stay_distinct():
    """An over-long producer key must not be TRUNCATED into the column.

    Two keys agreeing on their first 64 characters would truncate to one stored
    key, and the second call's charge would be dropped as a duplicate. The
    stored key must fit VARCHAR(64), stay distinct per distinct input, and stay
    STABLE across a redelivery — that last property is the whole point of rung 1.
    """
    from services._parsers import derive_event_key

    shared = "k" * 64
    longs = [shared + "-alpha", shared + "-beta", shared + ("z" * 200)]

    stored = []
    for i, raw in enumerate(longs):
        key, source = derive_event_key(
            {
                "tenant_id": "t",
                "service_key": "inference-router" if i % 2 else "w2v-tts",
                "event_key": raw,
            }
        )
        assert source == "producer"
        assert key is not None
        assert len(key) <= 64, f"{len(key)} chars will not fit VARCHAR(64)"
        # A condensed producer key must not be mistaken for a derived one.
        assert not key.startswith("d1:")
        # Stable across a re-emit of the same physical call, or it dedupes nothing.
        assert (
            derive_event_key(
                {
                    "tenant_id": "t",
                    "service_key": "inference-router" if i % 2 else "w2v-tts",
                    "event_key": raw,
                }
            )[0]
            == key
        )
        stored.append(key)

    assert len(set(stored)) == len(longs), "over-long keys collided into one stored key"

    # A key that already fits is still passed through verbatim, unchanged.
    fits = "k" * 64
    assert derive_event_key({"tenant_id": "t", "service_key": "w2v-tts", "event_key": fits}) == (
        fits,
        "producer",
    )
    assert stored[0] != fits and stored[1] != fits
