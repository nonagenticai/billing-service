"""Read-time list-price estimator for BYO transparency.

A BYO tenant runs on their OWN provider key, so metering has no authoritative
dollar cost for that usage (the platform never paid the bill). To still show
the tenant roughly what their provider is charging them, we resolve the public
LIST rate (``provider_pricing.rate_class = 'list'``) at read time and multiply
by the rolled-up token counts. This is explicitly an ESTIMATE — it never
touches the authoritative cost hot path (``rate_class = 'cost'``).

Built once per usage request (mirroring the markup resolver) so each cost line
resolves from an in-memory snapshot instead of its own DB round-trip. Reuses the
4-rung precedence of :func:`services.pricing._resolve_pricing_row` and the
per-row math of :func:`services.pricing._cost_from_row`.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import ProviderPricing
from services.pricing import _cost_from_row


class ListPricingResolver:
    """In-memory snapshot of currently-effective ``rate_class='list'`` rows.

    ``estimate(...)`` applies the same match precedence as the cost resolver:
      1. (provider, model, service_key)
      2. (provider, model, NULL)
      3. (provider, NULL, service_key)
      4. (provider, NULL, NULL)
    Returns ``None`` when no list row covers the provider (so callers can omit
    the estimate rather than show a misleading $0).
    """

    __slots__ = ("_by_provider",)

    def __init__(self, rows: list[ProviderPricing]) -> None:
        # Group by provider, newest-effective first, so the linear scan below
        # picks the current row when (unexpectedly) more than one is open.
        by_provider: dict[str, list[ProviderPricing]] = {}
        for row in sorted(rows, key=lambda r: r.effective_from, reverse=True):
            by_provider.setdefault(row.provider, []).append(row)
        self._by_provider = by_provider

    def _match(
        self, provider: str, served_model: str | None, service_key: str | None
    ) -> ProviderPricing | None:
        candidates = self._by_provider.get(provider)
        if not candidates:
            return None
        if served_model is not None and service_key is not None:
            for row in candidates:
                if row.model == served_model and row.service_key == service_key:
                    return row
        if served_model is not None:
            for row in candidates:
                if row.model == served_model and row.service_key is None:
                    return row
        if service_key is not None:
            for row in candidates:
                if row.model is None and row.service_key == service_key:
                    return row
        for row in candidates:
            if row.model is None and row.service_key is None:
                return row
        return None

    def estimate(
        self,
        *,
        provider: str | None,
        served_model: str | None,
        service_key: str | None,
        input_tokens: int,
        output_tokens: int,
        cache_creation_input_tokens: int = 0,
        cache_read_input_tokens: int = 0,
    ) -> Decimal | None:
        """Estimated public provider cost for this line, or None if uncovered.

        Note: ``cost_daily`` does not split cache tokens, so cache-aware
        providers (e.g. Anthropic) are approximated at their input rate — the
        estimate is deliberately a rough public-list figure, not an invoice.
        """
        if not provider:
            return None
        row = self._match(provider, served_model, service_key)
        if row is None:
            return None
        return _cost_from_row(
            row,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
        )


async def build_list_pricing_resolver(session: AsyncSession, ts: datetime) -> ListPricingResolver:
    """Load all ``rate_class='list'`` rows effective at ``ts`` into a resolver."""
    stmt = select(ProviderPricing).where(
        ProviderPricing.rate_class == "list",
        ProviderPricing.effective_from <= ts,
        or_(
            ProviderPricing.effective_until.is_(None),
            ProviderPricing.effective_until > ts,
        ),
    )
    rows = list((await session.execute(stmt)).scalars().all())
    return ListPricingResolver(rows)
