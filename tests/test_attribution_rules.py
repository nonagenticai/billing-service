"""billing_attribution_rules (alembic 0041): bill autonomous spend to a tenant member.

Operator policy: improvement streams (no fr project) bill as one member of the
fr-test tenant; execution projects bill as its execution member. All of them
arrive through one shared licence subject under a source tenant with
``actor_id`` NULL, so the rule — not the wire — decides the bill tenant and
member. These tests pin the matching, the precedence and the ingest behaviour.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import auth_deps
from models.db import (
    ACTIVE_LICENSE_STATUS,
    NULL_LICENSE_ID,
    NULL_PROJECT_ID,
    AttributionRule,
    CostEvent,
    RequestQuotaConfig,
    TokenUsage,
    cost_source_marks_refusal,
)
from services import attribution_audit as aa
from services import attribution_rules as attr
from services import license_resolver
from services.aggregator import flush_costs
from tests._aggregator_helpers import _make_cost
from tests.conftest import T


@pytest.fixture(autouse=True)
def _reset_rule_cache():
    attr.reset_cache()
    yield
    attr.reset_cache()


def _rule(
    *,
    src: str,
    bill: str,
    match: str = "any",
    subject: uuid.UUID | None = None,
    actor: uuid.UUID | None = None,
) -> attr.Rule:
    return attr.Rule(
        id=uuid.uuid4(),
        source_tenant_id=src,
        source_subject_id=subject,
        match_project=match,
        bill_tenant_id=bill,
        actor_id=actor,
    )


# --- pure matching ---------------------------------------------------------


@pytest.mark.unit
class TestSelectRule:
    def test_no_rule_for_tenant_returns_none(self) -> None:
        rules = {T("src"): (_rule(src=T("src"), bill=T("bill")),)}
        assert (
            attr.select_rule(rules, tenant_id=T("other"), subject_id=None, project_id=None) is None
        )

    def test_with_and_without_project_are_exclusive(self) -> None:
        src = T("src")
        with_p = _rule(src=src, bill=T("exec"), match="with_project")
        without_p = _rule(src=src, bill=T("impr"), match="without_project")
        rules = {src: (with_p, without_p)}
        proj = uuid.uuid4()
        assert attr.select_rule(rules, tenant_id=src, subject_id=None, project_id=proj) is with_p
        assert attr.select_rule(rules, tenant_id=src, subject_id=None, project_id=None) is without_p

    def test_placeholder_project_counts_as_without(self) -> None:
        src = T("src")
        with_p = _rule(src=src, bill=T("exec"), match="with_project")
        without_p = _rule(src=src, bill=T("impr"), match="without_project")
        rules = {src: (with_p, without_p)}
        got = attr.select_rule(rules, tenant_id=src, subject_id=None, project_id=NULL_PROJECT_ID)
        assert got is without_p

    def test_subject_specific_beats_tenant_wide(self) -> None:
        src, subj = T("src"), uuid.uuid4()
        # The tenant-wide rule is project-SPECIFIC, the subject rule is `any`:
        # subject specificity must still win.
        wide = _rule(src=src, bill=T("a"), match="without_project")
        specific = _rule(src=src, bill=T("b"), match="any", subject=subj)
        rules = {src: (wide, specific)}
        assert attr.select_rule(rules, tenant_id=src, subject_id=subj, project_id=None) is specific
        # A different subject falls back to the tenant-wide rule.
        assert (
            attr.select_rule(rules, tenant_id=src, subject_id=uuid.uuid4(), project_id=None) is wide
        )

    def test_project_specific_beats_any_at_same_subject_level(self) -> None:
        src = T("src")
        any_r = _rule(src=src, bill=T("a"), match="any")
        with_p = _rule(src=src, bill=T("b"), match="with_project")
        rules = {src: (any_r, with_p)}
        assert (
            attr.select_rule(rules, tenant_id=src, subject_id=None, project_id=uuid.uuid4())
            is with_p
        )
        assert attr.select_rule(rules, tenant_id=src, subject_id=None, project_id=None) is any_r

    def test_apply_preserves_existing_actor(self) -> None:
        real_actor, rule_actor = uuid.uuid4(), uuid.uuid4()
        row = {"tenant_id": T("src"), "actor_id": real_actor, "metadata": None}
        attr.apply_rule(row, _rule(src=T("src"), bill=T("bill"), actor=rule_actor))
        assert row["actor_id"] == real_actor
        assert row["tenant_id"] == T("bill")
        metadata = row["metadata"]
        assert metadata is not None
        assert metadata[attr.ATTRIBUTED_FROM_KEY] == T("src")


# --- ingest path (Postgres) ------------------------------------------------


async def _add_rule(factory, **kw) -> AttributionRule:
    async with factory() as s:
        row = AttributionRule(reason="test", **kw)
        s.add(row)
        await s.commit()
        await s.refresh(row)
        return row


async def _events(factory) -> list[CostEvent]:
    async with factory() as s:
        return list((await s.execute(select(CostEvent))).scalars().all())


@pytest.mark.unit
class TestFlushAppliesRules:
    async def test_no_rule_leaves_event_unchanged(self, db_session_factory) -> None:
        src = T("attr-src-none")
        await flush_costs([_make_cost(tenant_id=src)], db_session_factory, None)
        [ev] = await _events(db_session_factory)
        assert str(ev.tenant_id) == src
        assert ev.actor_id is None
        assert attr.ATTRIBUTED_FROM_KEY not in (ev.extra_metadata or {})

    async def test_without_project_rule_rebills_and_stamps_member(self, db_session_factory) -> None:
        src, bill, member = T("attr-src-1"), T("attr-fr-test"), uuid.uuid4()
        rule = await _add_rule(
            db_session_factory,
            source_tenant_id=src,
            match_project="without_project",
            bill_tenant_id=bill,
            actor_id=member,
        )
        await flush_costs(
            [
                _make_cost(tenant_id=src, input_tokens="10"),
                # Placeholder project must count as "without".
                _make_cost(tenant_id=src, input_tokens="20", project_id=str(NULL_PROJECT_ID)),
                # A real project does NOT match a without_project rule.
                _make_cost(tenant_id=src, input_tokens="30", project_id=str(uuid.uuid4())),
            ],
            db_session_factory,
            None,
        )
        by_in = {ev.input_tokens: ev for ev in await _events(db_session_factory)}
        for n in (10, 20):
            ev = by_in[n]
            assert str(ev.tenant_id) == bill
            assert ev.actor_id == member
            assert ev.extra_metadata is not None
            assert ev.extra_metadata[attr.ATTRIBUTED_FROM_KEY] == src
            assert ev.extra_metadata[attr.ATTRIBUTION_RULE_ID_KEY] == str(rule.id)
        assert str(by_in[30].tenant_id) == src
        assert by_in[30].actor_id is None

    async def test_with_project_rule_matches_real_project_only(self, db_session_factory) -> None:
        src, bill, member = T("attr-src-2"), T("attr-fr-test"), uuid.uuid4()
        await _add_rule(
            db_session_factory,
            source_tenant_id=src,
            match_project="with_project",
            bill_tenant_id=bill,
            actor_id=member,
        )
        await flush_costs(
            [
                _make_cost(tenant_id=src, input_tokens="10", project_id=str(uuid.uuid4())),
                _make_cost(tenant_id=src, input_tokens="20", project_id=str(NULL_PROJECT_ID)),
            ],
            db_session_factory,
            None,
        )
        by_in = {ev.input_tokens: ev for ev in await _events(db_session_factory)}
        assert str(by_in[10].tenant_id) == bill and by_in[10].actor_id == member
        assert str(by_in[20].tenant_id) == src and by_in[20].actor_id is None

    async def test_existing_actor_is_never_overwritten(self, db_session_factory) -> None:
        src, bill, real = T("attr-src-3"), T("attr-fr-test"), uuid.uuid4()
        await _add_rule(
            db_session_factory,
            source_tenant_id=src,
            match_project="any",
            bill_tenant_id=bill,
            actor_id=uuid.uuid4(),
        )
        await flush_costs([_make_cost(tenant_id=src, actor_id=str(real))], db_session_factory, None)
        [ev] = await _events(db_session_factory)
        assert str(ev.tenant_id) == bill
        assert ev.actor_id == real

    async def test_disabled_rule_is_ignored(self, db_session_factory) -> None:
        src = T("attr-src-4")
        await _add_rule(
            db_session_factory,
            source_tenant_id=src,
            match_project="any",
            bill_tenant_id=T("attr-fr-test"),
            actor_id=uuid.uuid4(),
            enabled=False,
        )
        await flush_costs([_make_cost(tenant_id=src)], db_session_factory, None)
        [ev] = await _events(db_session_factory)
        assert str(ev.tenant_id) == src
        assert ev.actor_id is None

    async def test_subject_specific_rule_wins_on_ingest(self, db_session_factory) -> None:
        src, subj = T("attr-src-5"), uuid.uuid4()
        wide_member, subj_member = uuid.uuid4(), uuid.uuid4()
        await _add_rule(
            db_session_factory,
            source_tenant_id=src,
            match_project="any",
            bill_tenant_id=T("attr-wide"),
            actor_id=wide_member,
        )
        await _add_rule(
            db_session_factory,
            source_tenant_id=src,
            source_subject_id=subj,
            match_project="any",
            bill_tenant_id=T("attr-subj"),
            actor_id=subj_member,
        )
        await flush_costs(
            [
                _make_cost(tenant_id=src, input_tokens="10", subject_id=str(subj)),
                _make_cost(tenant_id=src, input_tokens="20", subject_id=str(uuid.uuid4())),
            ],
            db_session_factory,
            None,
        )
        by_in = {ev.input_tokens: ev for ev in await _events(db_session_factory)}
        assert str(by_in[10].tenant_id) == T("attr-subj") and by_in[10].actor_id == subj_member
        assert str(by_in[20].tenant_id) == T("attr-wide") and by_in[20].actor_id == wide_member

    async def test_budget_rollup_stays_on_source_tenant(self, db_session_factory) -> None:
        """token_usage (budget enforcement) keys on the SOURCE, the ledger on the bill."""
        src, bill = T("attr-src-6"), T("attr-fr-test")
        await _add_rule(
            db_session_factory,
            source_tenant_id=src,
            match_project="any",
            bill_tenant_id=bill,
        )
        await flush_costs(
            [_make_cost(tenant_id=src, input_tokens="100", output_tokens="5")],
            db_session_factory,
            None,
        )
        async with db_session_factory() as s:
            usage = list((await s.execute(select(TokenUsage))).scalars().all())
        assert usage, "token_usage rollup did not run"
        assert {str(u.tenant_id) for u in usage} == {src}
        [ev] = await _events(db_session_factory)
        assert str(ev.tenant_id) == bill
        assert ev.license_id is not None and ev.license_id != NULL_LICENSE_ID


# --- ordering + audit invariants (Postgres) ---------------------------------


@pytest.fixture
def _cp_adjudication_env(monkeypatch):
    """Fresh licence-mirror cache, no service-key app map, no archival HTTP call.

    Same three fixtures ``tests/test_cp_license_adjudication.py`` uses, for the
    reasons stated there: a stale module-global owner map would make the seeded
    licence read ``unknown_license`` (a refusal produced by the fixture), and
    ``is_tenant_archived`` would otherwise reach for the licences service.
    """
    from settings import get_settings

    license_resolver.reset_cache()
    monkeypatch.setenv("COST_ATTRIBUTION_APP_BY_SERVICE_KEY", "")
    get_settings.cache_clear()

    async def _never_archived(_tenant_id, _settings):
        return False

    monkeypatch.setattr("services.cost_flusher.is_tenant_archived", _never_archived)
    yield
    license_resolver.reset_cache()
    get_settings.cache_clear()


@pytest.mark.integration
class TestAttributionRunsAfterLicenceAdjudication:
    async def test_source_tenants_own_licence_survives_re_tenanting(
        self, db_session_factory, _cp_adjudication_env
    ) -> None:
        """⚠️ MUTATION TARGET: the position of the attribution-rules block.

        The licence is asserted by a CP (adjudicated) emitter and is OWNED by the
        SOURCE tenant. Adjudication must judge it against the source tenant
        (accept). Moved before adjudication, the row is judged against the BILL
        tenant, refused ``foreign_tenant``, and license_id is nulled -- the spend
        silently falls off every invoice line.
        """
        src, bill = T("attr-order-src"), T("attr-fr-test")
        lic = uuid.uuid4()
        async with db_session_factory() as s:
            await s.execute(
                insert(RequestQuotaConfig),
                [
                    {
                        "license_id": lic,
                        "tenant_id": src,
                        "quota_monthly": 1000,
                        "app_name": "cp",
                        "status": ACTIVE_LICENSE_STATUS,
                    }
                ],
            )
            await s.commit()
        await _add_rule(
            db_session_factory,
            source_tenant_id=src,
            match_project="any",
            bill_tenant_id=bill,
            actor_id=uuid.uuid4(),
        )
        await flush_costs(
            [
                _make_cost(
                    tenant_id=src,
                    license_id=str(lic),
                    service_key="commonpractice",  # pragma: allowlist secret
                    cost_source="unknown",
                )
            ],
            db_session_factory,
            None,
        )
        [ev] = await _events(db_session_factory)
        assert str(ev.tenant_id) == bill, "rule did not re-tenant the row"
        assert ev.license_id == lic, "source tenant's own licence was refused/nulled"
        assert not cost_source_marks_refusal(ev.cost_source), ev.cost_source


@pytest.mark.integration
class TestAuditJudgesBySourceTenant:
    async def test_moved_row_is_not_an_ownership_or_multi_tenant_mismatch(
        self, db_session_factory, _cp_adjudication_env
    ) -> None:
        """⚠️ MUTATION TARGET: the ``attributed_from_tenant_id`` coalesce in
        ``_WINDOW_SQL`` and ``_PROJECT_MULTI_TENANT_SQL``.

        One project, one source tenant, two subjects: only one subject is
        re-tenanted by a rule. Judged by the STORED tenant, the licence reads as
        used by the bill tenant (ownership mismatch) and the project reads as
        shared by two tenants. Judged by the source tenant, neither.
        """
        src, bill, moved_subj = T("attr-audit-src"), T("attr-fr-test"), uuid.uuid4()
        lic, project = uuid.uuid4(), uuid.uuid4()
        await _add_rule(
            db_session_factory,
            source_tenant_id=src,
            source_subject_id=moved_subj,
            match_project="any",
            bill_tenant_id=bill,
            actor_id=uuid.uuid4(),
        )
        await flush_costs(
            [
                _make_cost(
                    tenant_id=src,
                    license_id=str(lic),
                    input_tokens="10",
                    project_id=str(project),
                    subject_id=str(moved_subj),
                ),
                _make_cost(
                    tenant_id=src,
                    license_id=str(lic),
                    input_tokens="20",
                    project_id=str(project),
                    subject_id=str(uuid.uuid4()),
                ),
            ],
            db_session_factory,
            None,
        )
        by_in = {ev.input_tokens: ev for ev in await _events(db_session_factory)}
        # Precondition: the fixture really did split the project across two
        # STORED tenants -- otherwise the audit assertions below are vacuous.
        assert str(by_in[10].tenant_id) == bill and str(by_in[20].tenant_id) == src

        floor = datetime.now(UTC) - timedelta(hours=1)
        async with db_session_factory() as s:
            window = (await s.execute(aa._WINDOW_SQL, {"floor": floor})).all()
            multi = (await s.execute(aa._PROJECT_MULTI_TENANT_SQL, {"floor": floor})).all()
        used_by = {r.tenant_id.lower() for r in window if r.license_id == str(lic)}
        assert used_by == {src.lower()}, f"licence judged against bill tenant: {used_by}"
        assert [r for r in multi if r.project_id == project] == [], multi


# --- admin API --------------------------------------------------------------


@pytest.fixture
def writer_actor(monkeypatch):
    fake_payload = {"sub": "test-admin-user", "realm_access": {"roles": ["license-admin"]}}

    async def _fake_decode(_request):
        return fake_payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


@pytest.mark.unit
class TestAttributionRuleRoutes:
    PATH = "/api/config/attribution-rules"
    H = {"Authorization": "Bearer fake"}

    async def test_put_upserts_on_match_tuple_then_delete(
        self, app_client: TestClient, db_engine, writer_actor
    ) -> None:
        member = str(uuid.uuid4())
        body = {
            "source_tenant_id": T("api-src"),
            "match_project": "without_project",
            "bill_tenant_id": T("api-fr-test"),
            "actor_id": member,
            "reason": "improvement streams -> fr-test member",
        }
        r1 = app_client.put(self.PATH, headers=self.H, json=body)
        assert r1.status_code == 200, r1.text
        rid = r1.json()["data"]["id"]
        r2 = app_client.put(self.PATH, headers=self.H, json={**body, "enabled": False})
        assert r2.status_code == 200, r2.text
        assert r2.json()["data"]["id"] == rid, "same match tuple must update, not insert"
        assert r2.json()["data"]["enabled"] is False

        listed = app_client.get(self.PATH, headers=self.H).json()["data"]
        assert [x["id"] for x in listed] == [rid]

        rd = app_client.delete(f"{self.PATH}/{rid}", headers=self.H)
        assert rd.status_code == 204, rd.text
        assert app_client.get(self.PATH, headers=self.H).json()["data"] == []
        assert app_client.delete(f"{self.PATH}/{rid}", headers=self.H).status_code == 404

        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as s:
            from models.db import AuditLog

            actions = [
                a.action
                for a in (await s.execute(select(AuditLog))).scalars().all()
                if a.action.startswith("config.attribution_rule.")
            ]
        assert actions.count("config.attribution_rule.upsert") == 2
        assert actions.count("config.attribution_rule.delete") == 1

    async def test_unregistered_bill_tenant_is_refused(
        self, app_client: TestClient, writer_actor
    ) -> None:
        resp = app_client.put(
            self.PATH,
            headers=self.H,
            json={
                "source_tenant_id": T("api-src"),
                "match_project": "any",
                "bill_tenant_id": str(uuid.uuid4()),
                "reason": "x",
            },
        )
        assert resp.status_code == 422, resp.text
