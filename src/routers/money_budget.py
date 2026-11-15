"""Money-budget endpoints (alembic 0044/0045; PLAN rev E §3.1 + REV F multi-currency).

Budget currency is EUR | USD | CAD; every amount in every body is in the
``currency`` named beside it.

Mounted under the same ``/api/budget`` prefix as :mod:`routers.budget` (tag
``budget``) but kept in its own module so the token-budget router is untouched.

⛔ CROSS-REPO CONTRACT: inference-router calls ``POST /money/reserve``;
provisioning/fr call ``PUT /money/{tenant_id}``, ``GET /money/quote`` and
``GET /money/status/{tenant_id}``; fr (owner) calls
``POST /money/{tenant_id}/currency``; CommonPractice calls ``POST
/money/topup`` (invoiced PREPAID CREDIT, alembic 0055) and ``GET
/money/credit/{tenant_id}``. Shapes are pinned by
``tests/test_money_budget_reserve.py`` and ``tests/test_money_budget_put.py``.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from auth_deps import (
    enforce_tenant_isolation,
    require_internal,
    require_money_topup_writer,
    require_owner_via_service,
    require_tenant,
    resolve_tenant_or_400,
)
from db import get_session
from models.db import MoneyBudget
from models.domain import (
    MONEY_RESERVE_REASONS,
    Envelope,
    MoneyBudgetPut,
    MoneyBudgetPutResponse,
    MoneyBudgetStatus,
    MoneyConsumedSegment,
    MoneyCurrency,
    MoneyCurrencyChange,
    MoneyCurrencyChangeResponse,
    MoneyCurrencyConversion,
    MoneyQuote,
    MoneyQuoteRefusal,
    MoneyReserveRequest,
    MoneyReserveResponse,
    MoneyTopupInvoiceLine,
    MoneyTopupRequest,
    MoneyTopupResponse,
    PrepaidCreditBalance,
    envelope,
)
from observability.metrics import money_reserve_refused_total
from redis_client import get_redis
from services import money_budget as money_svc
from services import prepaid_credit as credit_svc
from services.audit import record as audit_record
from services.tenant_names import (
    TenantRegistryUnavailableError,
    get_tenant_info_strict,
    is_tenant_archived,
)
from settings import get_settings
from tenancy import redact_tenant_id

logger = structlog.stdlib.get_logger(__name__)

router = APIRouter(prefix="/api/budget", tags=["budget"])


# All seven routes 404 while BILLING_MONEY_BUDGETS_ENABLED is off.


def _ccy(value: str) -> MoneyCurrency:
    """Narrow a stored ISO code to the wire Literal (the DB CHECK holds the same set)."""
    code = value.strip()
    if code not in money_svc.SUPPORTED_CURRENCIES:
        raise ValueError(f"unsupported money currency {value!r}")
    return cast("MoneyCurrency", code)


def _require_money_enabled() -> None:
    if not get_settings().billing_money_budgets_enabled:
        raise HTTPException(status_code=404, detail="Not Found")


def _money_refusal_response(body: dict) -> JSONResponse:
    """The §3.1 429: NOT enveloped, ``{"detail": {...}}`` exactly.

    Returned directly rather than raised: the app's HTTPException handler
    re-wraps a dict detail as ``{"error": {"details": ...}}``, which is not the
    contract shape. (IR's parser accepts both; the contract names this one.)
    """
    reason = body.get("reason")
    money_reserve_refused_total.labels(
        reason=reason if reason in MONEY_RESERVE_REASONS else "other"
    ).inc()
    return JSONResponse(status_code=429, content={"detail": body})


@router.post(
    "/money/reserve",
    response_model=Envelope[MoneyReserveResponse],
    operation_id="reserve_money_budget",
    responses={
        429: {
            "description": (
                'Refused. NOT enveloped: {"detail": MoneyReserveResponse} with '
                "reason over_budget | no_active_budget | tenant_archived | "
                "fx_rate_missing | price_missing and the tenant's real amounts."
            ),
        },
        503: {"description": "No Redis to hold the reservation in (billing outage)."},
    },
)
async def reserve_money_budget(
    request: Request,
    body: MoneyReserveRequest,
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
):
    """EUR pre-flight hold for one IR hop: ``consumed + in_flight + est <= amount``.

    Idempotent per ``(inference_id, hop_index)``: a retry while that hop's hold
    is live returns the same ``reservation_id`` and holds nothing more.

    ``no_active_budget`` is a HARD STOP here, unlike ``/reserve`` (tokens).
    There is no commit call: the cost event carrying ``reservation_id`` settles
    and releases the hold; an unsettled hold expires at ``ttl_seconds``.
    """
    _require_money_enabled()
    tenant = _tenant
    enforce_tenant_isolation(request, tenant, tenant)
    redis_conn = get_redis()

    # Archival is an off-switch here exactly as on the token path (see
    # reserve_tenant_budget): fail-open on an unknown, refuse on a known archive.
    if await is_tenant_archived(
        tenant,
        get_settings(),
        getattr(request.app.state, "service_token_provider", None),
    ):
        refused = await money_svc.snapshot_refusal(
            session, redis_conn, tenant_id=tenant, reason="tenant_archived"
        )
        logger.info("money_reserve_refused_tenant_archived", tenant_id=redact_tenant_id(tenant))
        return _money_refusal_response(refused)

    try:
        result = await money_svc.reserve_money(
            session,
            redis_conn,
            tenant_id=tenant,
            setup=body.setup,
            provider=body.provider,
            model=body.model,
            byo=body.byo,
            input_tokens=body.estimated_input_tokens,
            output_tokens=body.max_output_tokens,
            ttl_seconds=body.ttl_seconds,
            inference_id=body.inference_id,
            hop_index=body.hop_index,
        )
    except money_svc.MoneyRedisUnavailable as exc:
        raise HTTPException(status_code=503, detail="redis_unavailable") from exc
    if not result["allowed"]:
        logger.info(
            "money_reserve_refused",
            tenant_id=redact_tenant_id(tenant),
            reason=result["reason"],
            setup=body.setup,
            hop_index=body.hop_index,
        )
        return _money_refusal_response(result)
    response = {k: v for k, v in result.items() if k != "reason"}
    return envelope(MoneyReserveResponse(**response), request)


@router.get(
    "/money/quote",
    response_model=MoneyQuote,
    operation_id="quote_money_budget",
    responses={422: {"model": MoneyQuoteRefusal}},
)
async def quote_money_budget(
    tenant_id: str = Query(),
    setup: int = Query(ge=1, le=3),
    provider: str = Query(min_length=1, max_length=64),
    model: str = Query(min_length=1, max_length=128),
    input_tokens: int = Query(ge=0),
    output_tokens: int = Query(ge=0),
    currency: MoneyCurrency | None = Query(
        default=None,
        description="Defaults to the tenant's budget currency, else EUR (no budget yet).",
    ),
    session: AsyncSession = Depends(get_session),
    _caller: dict = Depends(require_internal),
):
    """Price of ``input_tokens``/``output_tokens`` for this tenant and target.

    In ``currency`` when given, else the tenant's active budget currency, else
    EUR. Provisioning calls this before the IR policy PUT. 422
    ``{"reason": ...}`` = price_missing | fx_rate_missing | markup_missing.
    """
    _require_money_enabled()
    tenant = resolve_tenant_or_400(tenant_id)
    if currency is None:
        budget = await money_svc.get_active_money_budget(session, tenant)
        currency = _ccy(budget.currency if budget is not None else money_svc.DEFAULT_CURRENCY)
    try:
        priced = await money_svc.price_in_currency(
            session,
            currency=currency,
            tenant_id=tenant,
            setup=setup,
            provider=provider,
            model=model,
            byo=setup == 3,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            ts=datetime.now(UTC),
        )
    except money_svc.MoneyRefusal as refusal:
        return JSONResponse(status_code=422, content={"reason": refusal.reason})
    return MoneyQuote(
        currency=_ccy(priced.currency),
        amount=money_svc.fmt8(priced.amount),
        markup_pct=f"{priced.markup_pct.quantize(Decimal('0.01'))}",
        fx_rate=f"{priced.fx_rate.normalize():f}",
    )


@router.put(
    "/money/{tenant_id}",
    response_model=MoneyBudgetPutResponse,
    operation_id="put_money_budget",
    responses={
        409: {
            "description": 'Currency differs from the active budget\'s: {"detail":'
            '{"reason":"currency_change_requires_conversion","current_currency",'
            '"requested_currency","hint"}}. Nothing is written.'
        }
    },
)
async def put_money_budget(
    request: Request,
    tenant_id: str,
    body: MoneyBudgetPut,
    session: AsyncSession = Depends(get_session),
    _caller: dict = Depends(require_internal),
):
    """Set the tenant's whole-unit budget (idempotent) and flip budget_unit='money'.

    A replay with the same body changes nothing and returns the same
    ``budget_id``; a new amount updates the ONE active row in place.
    ``currency`` creates the budget in that currency; a different currency than
    the active budget's is a 409 ``currency_change_requires_conversion`` (use
    ``POST /money/{tenant_id}/currency``, owner only).
    """
    _require_money_enabled()
    tenant = resolve_tenant_or_400(tenant_id)
    try:
        budget, before = await money_svc.put_money_budget(
            session,
            tenant,
            amount=body.amount,
            period=body.period,
            source=body.source,
            currency=body.currency,
        )
    except money_svc.MoneyCurrencyMismatch as exc:
        await session.rollback()
        return JSONResponse(
            status_code=409,
            content={
                "detail": {
                    "reason": "currency_change_requires_conversion",
                    "current_currency": exc.current,
                    "requested_currency": exc.requested,
                    "hint": f"POST /api/budget/money/{tenant}/currency (owner only)",
                }
            },
        )
    after = money_svc.money_budget_audit_dict(budget)
    if before != after:
        await audit_record(
            session,
            request,
            action="budget.money.put",
            resource_type="tenant_money_budget",
            resource_id=str(budget.id),
            tenant_id=tenant,
            before=before,
            after=after,
        )
    await session.commit()
    return MoneyBudgetPutResponse(
        budget_id=str(budget.id), amount=int(budget.amount), currency=_ccy(budget.currency)
    )


@router.get(
    "/money/status/{tenant_id}",
    response_model=MoneyBudgetStatus,
    operation_id="get_money_budget_status",
    responses={404: {"description": "No active money budget for this tenant."}},
)
async def money_budget_status(
    request: Request,
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
):
    """Current-period EUR position: amount, consumed, remaining, thresholds crossed."""
    _require_money_enabled()
    enforce_tenant_isolation(request, tenant_id, _tenant)
    tenant = resolve_tenant_or_400(tenant_id)
    budget = await money_svc.get_active_money_budget(session, tenant)
    if budget is None:
        raise HTTPException(status_code=404, detail="no_active_money_budget")
    return await _status_of(session, budget)


async def _status_of(session: AsyncSession, budget: MoneyBudget) -> MoneyBudgetStatus:
    """The status body for one budget row: the cap, plus the prepaid credit."""
    now = datetime.now(UTC)
    consumed = money_svc.effective_money_consumed(budget, now)
    amount = int(budget.amount)
    conversions = await money_svc.period_conversions(session, budget, now)
    return MoneyBudgetStatus(
        amount=amount,
        currency=_ccy(budget.currency),
        consumed=money_svc.fmt8(consumed),
        remaining=money_svc.fmt8(Decimal(amount) - consumed),
        thresholds_crossed=money_svc.thresholds_crossed(amount, consumed),
        consumed_by_currency=[
            MoneyConsumedSegment(currency=_ccy(c), consumed=money_svc.fmt8(v))
            for c, v in money_svc.consumed_by_currency(budget, consumed, conversions)
        ],
        conversions=[
            MoneyCurrencyConversion(
                adjustment_id=str(a.id),
                at=a.created_at,
                from_currency=_ccy(a.from_currency),
                to_currency=_ccy(a.to_currency),
                fx_rate=f"{Decimal(a.fx_rate).normalize():f}",
                amount_before=int(a.amount_before),
                amount_after=int(a.amount_after),
                consumed_before=money_svc.fmt8(Decimal(a.consumed_before)),
                remaining_before=money_svc.fmt8(Decimal(a.remaining_before)),
                remaining_after=money_svc.fmt8(Decimal(a.remaining_after)),
            )
            for a in conversions
        ],
        credit=await _credit_of(session, str(budget.tenant_id)),
    )


async def _credit_of(session: AsyncSession, tenant_id: str) -> PrepaidCreditBalance:
    bal = await credit_svc.credit_balance(session, tenant_id)
    return PrepaidCreditBalance(
        granted_usd=str(bal.granted_usd),
        drawn_usd=str(bal.drawn_usd),
        remaining_usd=str(bal.remaining_usd),
        settled_through=bal.settled_through,
    )


@router.get(
    "/money/credit/{tenant_id}",
    response_model=PrepaidCreditBalance,
    operation_id="get_prepaid_credit",
)
async def prepaid_credit_balance(
    request: Request,
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
):
    """The tenant's PREPAID CREDIT balance, in USD, from the credit ledger.

    Needs no money budget: credit and the money-budget cap are independent.
    A tenant that never topped up reads all zeros. Same gate as ``status``.
    """
    _require_money_enabled()
    enforce_tenant_isolation(request, tenant_id, _tenant)
    return await _credit_of(session, resolve_tenant_or_400(tenant_id))


@router.post(
    "/money/topup",
    response_model=MoneyTopupResponse,
    operation_id="topup_money_budget",
    responses={
        401: {"description": "No verifiable bearer token."},
        403: {
            "description": "Caller lacks license-admin and is not an allow-listed top-up SA "
            '({"detail":{"reason":"topup_writer_required"}}).'
        },
        404: {
            "description": 'The feature flag is off, or {"detail":{"reason":'
            '"tenant_unregistered"}}: the tenant is not in the licences registry.'
        },
        409: {
            "description": '{"detail":{"reason":"idempotency_key_reused",...}}: same key, '
            'different amount/currency; or {"detail":{"reason":"tenant_archived"}}. '
            "Nothing written."
        },
        422: {"description": '{"reason":"fx_rate_missing"} -- nothing written.'},
        503: {"description": '{"detail":{"reason":"tenant_registry_unavailable"}}.'},
    },
)
async def topup_money_budget(
    request: Request,
    body: MoneyTopupRequest,
    session: AsyncSession = Depends(get_session),
    caller: dict = Depends(require_money_topup_writer),
):
    """Buy PREPAID CREDIT and INVOICE it -- once per ``idempotency_key``.

    One transaction: a ``topup`` credit entry on the tenant's credit ledger (USD
    at the FX rate effective now); a ``cadence='one_time'`` access charge
    (service_key ``topup:<topup_id>``) that the invoice generator itemises in
    this month; an audit row. Later statements net their LLM usage against the
    credit and bill only usage beyond it. The money-budget cap is NOT touched.
    A replay with the same key and body returns the original top-up with
    ``replayed: true`` and writes nothing.
    """
    _require_money_enabled()
    tenant = body.tenant_id
    # ⛔ A top-up puts a charge on an invoice, so it asks the SAME question the
    # invoice close asks, from the same STRICT registry read
    # (services.invoices.assert_tenant_invoiceable): an unknown or archived tenant
    # could never be invoiced for it, and a registry outage must not be read as
    # "unknown" (a degraded {} makes every tenant look unregistered).
    try:
        registry = await get_tenant_info_strict(
            get_settings(), getattr(request.app.state, "service_token_provider", None)
        )
    except TenantRegistryUnavailableError:
        return JSONResponse(
            status_code=503, content={"detail": {"reason": "tenant_registry_unavailable"}}
        )
    info = registry.get(tenant.lower())
    if info is None:
        return JSONResponse(status_code=404, content={"detail": {"reason": "tenant_unregistered"}})
    if info.archived:
        return JSONResponse(status_code=409, content={"detail": {"reason": "tenant_archived"}})
    actor = getattr(request.state, "audit_actor", None) or {}
    try:
        result = await credit_svc.topup_credit(
            session,
            tenant,
            amount=body.amount,
            currency=body.currency,
            idempotency_key=body.idempotency_key,
            actor_sub=actor.get("sub"),
            actor_role=actor.get("role"),
            via_client=caller.get("azp") or caller.get("client_id"),
        )
    except credit_svc.TopupConflict as exc:
        await session.rollback()
        return JSONResponse(
            status_code=409, content={"detail": {"reason": exc.reason, **exc.detail}}
        )
    except money_svc.MoneyRefusal as refusal:
        await session.rollback()
        return JSONResponse(status_code=422, content={"reason": refusal.reason})
    entry = result.entry
    if not result.replayed:
        await audit_record(
            session,
            request,
            action="credit.topup",
            resource_type="tenant_credit_ledger",
            resource_id=str(entry.id),
            tenant_id=tenant,
            before=None,
            after={
                "kind": entry.kind,
                "entry_key": entry.entry_key,
                "amount_usd": str(entry.amount_usd),
                "source_amount": entry.source_amount,
                "source_currency": entry.source_currency,
                "fx_rate": f"{Decimal(str(entry.fx_rate)).normalize():f}",
                "access_charge_id": result.charge.id,
            },
        )
        await session.commit()
        logger.info(
            "prepaid_credit_topped_up",
            tenant_id=redact_tenant_id(tenant),
            amount=entry.source_amount,
            currency=entry.source_currency,
            amount_usd=str(entry.amount_usd),
        )
    charge = result.charge
    return MoneyTopupResponse(
        topup_id=str(entry.id),
        tenant_id=str(entry.tenant_id),
        idempotency_key=body.idempotency_key,
        replayed=result.replayed,
        amount=int(entry.source_amount or 0),
        currency=_ccy(str(entry.source_currency)),
        credit_usd=f"{Decimal(entry.amount_usd).quantize(Decimal('0.01'))}",
        invoice_line=MoneyTopupInvoiceLine(
            access_charge_id=int(charge.id),
            service_key=str(charge.service_key),
            amount_usd=f"{Decimal(charge.amount_usd).quantize(Decimal('0.01'))}",
            fx_rate=f"{Decimal(str(entry.fx_rate)).normalize():f}",
            effective_from=charge.effective_from,
        ),
        created_at=entry.created_at,
        credit=await _credit_of(session, tenant),
    )


@router.post(
    "/money/{tenant_id}/currency",
    response_model=MoneyCurrencyChangeResponse,
    operation_id="change_money_budget_currency",
    responses={
        401: {
            "description": "X-Forwarded-User-Authorization missing or unverifiable "
            '({"detail":{"reason":"forwarded_user_token_invalid"}}).'
        },
        403: {
            "description": "SA lacks an internal billing role, or the forwarded portal "
            'user lacks super_admin / platform-admin ({"detail":{"reason":'
            '"owner_role_required"}}).'
        },
        404: {"description": "No active money budget (or the feature flag is off)."},
        409: {
            "description": 'Reservations are open: {"detail":{"reason":"holds_in_flight",'
            '"holds":N}}. Nothing written; retry once they settle or expire (<= 600 s).'
        },
        422: {"description": '{"reason":"fx_rate_missing"} -- nothing written.'},
        503: {"description": "Redis unavailable: open holds cannot be counted."},
    },
)
async def change_money_budget_currency(
    request: Request,
    tenant_id: str,
    body: MoneyCurrencyChange,
    session: AsyncSession = Depends(get_session),
    owner: dict = Depends(require_owner_via_service),
):
    """Change a tenant's budget currency mid-period (OWNER only, audited, immediate).

    Caller: an internal SA (``onboarding-internal`` -- provisioning) relaying the
    owner's portal JWT in ``X-Forwarded-User-Authorization`` (realm role
    ``super_admin`` or ``platform-admin``); see ``require_owner_via_service``.

    Converts the REMAINING budget at the newest FX rates effective now, cross
    ``rate(USD->to) / rate(USD->from)`` (USD = 1), ROUND_HALF_UP to minor units;
    the cap ROUND_HALF_UP to whole units. Records a
    ``tenant_money_budget_adjustment`` row (before-figures in the original
    currency, rate used, owner + relaying SA) and an ``audit_log`` row (actor =
    owner sub; ``via`` = SA azp). Refused while reservations are open (409
    ``holds_in_flight``). Same currency = 200 ``changed:false``, nothing written.
    """
    _require_money_enabled()
    tenant = resolve_tenant_or_400(tenant_id)
    try:
        change = await money_svc.change_money_currency(
            session,
            get_redis(),
            tenant,
            to_currency=body.currency,
            actor_sub=owner["owner_sub"],
            actor_role=owner["owner_role"],
            via_client=owner["via"],
            reason=body.reason,
        )
    except money_svc.MoneyRefusal as refusal:
        await session.rollback()
        return JSONResponse(status_code=422, content={"reason": refusal.reason})
    except money_svc.MoneyHoldsInFlight as exc:
        await session.rollback()
        return JSONResponse(
            status_code=409, content={"detail": {"reason": "holds_in_flight", "holds": exc.holds}}
        )
    except money_svc.MoneyRedisUnavailable as exc:
        await session.rollback()
        raise HTTPException(status_code=503, detail="redis_unavailable") from exc
    if change is None:
        raise HTTPException(status_code=404, detail="no_active_money_budget")
    budget = change.budget
    if change.changed:
        assert change.adjustment is not None
        await audit_record(
            session,
            request,
            action="budget.money.currency_change",
            resource_type="tenant_money_budget",
            resource_id=str(budget.id),
            tenant_id=tenant,
            before=change.before_audit,
            after={
                **money_svc.money_budget_audit_dict(budget),
                "adjustment_id": str(change.adjustment.id),
                "from_currency": change.from_currency,
                "to_currency": change.to_currency,
                "fx_rate": f"{change.fx_rate.normalize():f}",
                "amount_before": change.amount_before,
                "remaining_before": money_svc.fmt8(change.remaining_before),
                "remaining_after": money_svc.fmt8(change.remaining_after),
                "via": owner["via"],
                "reason": body.reason,
            },
        )
    await session.commit()

    holds: int | None = 0
    if change.changed:
        redis_conn = get_redis()
        try:
            holds = (
                await money_svc.convert_money_holds(
                    redis_conn,
                    tenant,
                    from_currency=change.from_currency,
                    to_currency=change.to_currency,
                    rate=change.fx_rate,
                )
                if redis_conn is not None
                else None
            )
        except Exception:  # noqa: BLE001 - committed; the next reserve self-heals
            logger.warning(
                "money_currency_change_holds_not_converted", tenant_id=redact_tenant_id(tenant)
            )
            holds = None
    logger.info(
        "money_currency_changed" if change.changed else "money_currency_unchanged",
        tenant_id=redact_tenant_id(tenant),
        from_currency=change.from_currency,
        to_currency=change.to_currency,
        holds_converted=holds,
    )
    return MoneyCurrencyChangeResponse(
        changed=change.changed,
        budget_id=str(budget.id),
        adjustment_id=str(change.adjustment.id) if change.adjustment is not None else None,
        from_currency=_ccy(change.from_currency),
        to_currency=_ccy(change.to_currency),
        fx_rate=f"{change.fx_rate.normalize():f}",
        amount_before=change.amount_before,
        amount=int(budget.amount),
        consumed_before=money_svc.fmt8(change.consumed_before),
        consumed=money_svc.fmt8(
            Decimal(budget.consumed) if change.changed else change.consumed_before
        ),
        remaining_before=money_svc.fmt8(change.remaining_before),
        remaining_after=money_svc.fmt8(change.remaining_after),
        holds_converted=holds,
    )
