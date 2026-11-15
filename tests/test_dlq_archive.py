"""Archive-before-trim for ``billing:costs:dead`` (#243).

Redis is a quarantined cost event's ONLY copy, and the 720h age trim used to
destroy it: 11 entries were lost 2026-09-30..10-02 while
BillingCostDeadLetterNearExpiry fired. ``archive_then_trim_dead_letter`` commits
every expiring entry to ``billing_dlq_archive`` and only THEN trims. These tests
pin the ordering: a failed archive must leave Redis untouched.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from observability import metrics as om
from services.dlq_ops import archive_then_trim_dead_letter

DLQ = "billing:costs:dead"

# The on-wire shape of the 3 entries live on 2026-10-04 (nil tenant, commonpractice).
_SURVIVOR = {
    "tenant_id": "00000000-0000-0000-0000-000000000000",
    "service_key": "commonpractice",  # pragma: allowlist secret
    "stage": "content-translation",
    "provider": "pgx-local",
    "model": "local-llm",
    "cost_usd": "0.00097000",
    "timestamp": "2026-09-02T13:09:34.336Z",
    "_dlq_reason": "nil_tenant_id",
    "_dlq_original_id": "1788354574343-0",
    "_dlq_at": "2026-09-02T13:09:35.713329+00:00",
}


async def _archived(factory) -> list[tuple[str, dict]]:
    async with factory() as session:
        rows = await session.execute(
            text(
                "SELECT dead_entry_id, fields FROM billing_dlq_archive "
                "WHERE stream = :s ORDER BY dead_entry_id"
            ),
            {"s": DLQ},
        )
        return [(r[0], r[1]) for r in rows.all()]


def _expired_count() -> float:
    return om.cost_dead_letter_expired_total.labels(stream=DLQ)._value.get()


@pytest.mark.integration
async def test_expired_entries_are_archived_then_removed_fresh_ones_untouched(
    fake_redis, db_session_factory
):
    await fake_redis.xadd(DLQ, _SURVIVOR, id="1788354575728-1")
    await fake_redis.xadd(DLQ, {"payload": "old2"}, id="1788354579338-0")
    fresh_id = await fake_redis.xadd(DLQ, {"payload": "fresh"}, id="*")
    before = _expired_count()

    removed = await archive_then_trim_dead_letter(fake_redis, db_session_factory, DLQ, 720)

    assert removed == 2
    assert _expired_count() - before == 2
    assert [i for i, _ in await fake_redis.xrange(DLQ)] == [fresh_id]
    archived = await _archived(db_session_factory)
    assert archived == [
        ("1788354575728-1", _SURVIVOR),
        ("1788354579338-0", {"payload": "old2"}),
    ], "every removed entry must be archived verbatim, and nothing else"
    async with db_session_factory() as session:
        reason = (
            await session.execute(
                text("SELECT dlq_reason FROM billing_dlq_archive WHERE dead_entry_id = :i"),
                {"i": "1788354575728-1"},
            )
        ).scalar_one()
    assert reason == "nil_tenant_id"


@pytest.mark.integration
async def test_a_failed_archive_trims_nothing(fake_redis):
    """MUST-FAIL CONTROL: if the archive cannot be written, Redis keeps the entry."""
    await fake_redis.xadd(DLQ, _SURVIVOR, id="1-0")

    class _Boom:
        def __call__(self):
            raise RuntimeError("db down")

    with pytest.raises(RuntimeError, match="db down"):
        await archive_then_trim_dead_letter(fake_redis, _Boom(), DLQ, 720)

    assert [i for i, _ in await fake_redis.xrange(DLQ)] == ["1-0"], (
        "an entry that was not archived must never be trimmed"
    )


@pytest.mark.integration
async def test_rearchiving_is_a_no_op(fake_redis, db_session_factory):
    """An entry already archived (e.g. trim interrupted after commit) re-archives cleanly."""
    await fake_redis.xadd(DLQ, _SURVIVOR, id="1-0")
    async with db_session_factory() as session:
        await session.execute(
            text(
                "INSERT INTO billing_dlq_archive (stream, dead_entry_id, fields) "
                "VALUES (:s, '1-0', CAST(:f AS jsonb))"
            ),
            {"s": DLQ, "f": '{"prior": "copy"}'},
        )
        await session.commit()

    removed = await archive_then_trim_dead_letter(fake_redis, db_session_factory, DLQ, 720)

    assert removed == 1
    assert await fake_redis.xlen(DLQ) == 0
    assert await _archived(db_session_factory) == [("1-0", {"prior": "copy"})]


@pytest.mark.integration
async def test_nothing_expired_touches_neither_store(fake_redis, db_session_factory):
    fresh_id = await fake_redis.xadd(DLQ, {"payload": "fresh"}, id="*")

    assert await archive_then_trim_dead_letter(fake_redis, db_session_factory, DLQ, 720) == 0
    assert [i for i, _ in await fake_redis.xrange(DLQ)] == [fresh_id]
    assert await _archived(db_session_factory) == []


@pytest.mark.unit
async def test_no_redis_is_a_no_op():
    assert await archive_then_trim_dead_letter(None, None, DLQ, 720) == 0


@pytest.mark.integration
async def test_a_backlog_larger_than_one_batch_is_archived_in_bounded_batches(
    fake_redis, db_session_factory, monkeypatch
):
    """An unbounded multi-row INSERT wedges at ~10.9k rows (65535-parameter cap)
    and every later tick retries a bigger set. Batching must drain any size."""
    import services.dlq_ops as dlq_ops

    monkeypatch.setattr(dlq_ops, "_ARCHIVE_BATCH", 3)
    batch_sizes: list[int] = []
    real_insert = dlq_ops._insert_archive_rows

    async def _spy(factory, rows):
        batch_sizes.append(len(rows))
        await real_insert(factory, rows)

    monkeypatch.setattr(dlq_ops, "_insert_archive_rows", _spy)
    # 3-0 ends batch 1 and 3-1 (same millisecond) starts batch 2: the exact
    # MINID of batch 1 must not take batch 2's not-yet-archived seq.
    ids = ["1-0", "2-0", "3-0", "3-1", "4-0", "5-0", "6-0", "7-0"]
    for entry_id in ids:
        await fake_redis.xadd(DLQ, {"id": entry_id}, id=entry_id)

    removed = await archive_then_trim_dead_letter(fake_redis, db_session_factory, DLQ, 720)

    assert batch_sizes == [3, 3, 2], "every INSERT must be bounded by _ARCHIVE_BATCH"
    assert removed == 8
    assert await fake_redis.xlen(DLQ) == 0
    assert [i for i, _ in await _archived(db_session_factory)] == ids


@pytest.mark.integration
async def test_a_nul_byte_entry_does_not_wedge_the_trim(fake_redis, db_session_factory):
    """Postgres cannot store NUL in text/jsonb; a malformed entry carrying one
    must still be archived (escaped) and trimmed, along with its neighbours."""
    await fake_redis.xadd(DLQ, {"payload": "ok-before"}, id="1-0")
    await fake_redis.xadd(DLQ, {"payload": "bad\x00value", "_dlq_reason": "x\x00y"}, id="2-0")
    await fake_redis.xadd(DLQ, {"payload": "ok-after"}, id="3-0")

    removed = await archive_then_trim_dead_letter(fake_redis, db_session_factory, DLQ, 720)

    assert removed == 3
    archived = dict(await _archived(db_session_factory))
    assert archived["2-0"] == {"payload": "bad\\u0000value", "_dlq_reason": "x\\u0000y"}
    assert archived["1-0"] == {"payload": "ok-before"}
    assert archived["3-0"] == {"payload": "ok-after"}


@pytest.mark.integration
async def test_the_cutoff_boundary(fake_redis, db_session_factory):
    """Just inside the age bound is archived+trimmed; just outside is untouched."""
    import time

    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - 720 * 3600 * 1000
    await fake_redis.xadd(DLQ, {"payload": "expired"}, id=f"{cutoff_ms - 5000}-0")
    young = f"{cutoff_ms + 60_000}-0"
    await fake_redis.xadd(DLQ, {"payload": "young"}, id=young)

    removed = await archive_then_trim_dead_letter(fake_redis, db_session_factory, DLQ, 720)

    assert removed == 1
    assert [i for i, _ in await fake_redis.xrange(DLQ)] == [young]
    assert [i for i, _ in await _archived(db_session_factory)] == [f"{cutoff_ms - 5000}-0"]
