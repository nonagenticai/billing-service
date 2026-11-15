"""Shared SQL aggregation helpers for the usage + costs routers.

The two routers ran six near-identical ``SELECT ... GROUP BY ... SUM(...)``
blocks against ``metering_hourly`` / ``metering_daily`` / ``cost_daily``.
This module factors out:

- :func:`granularity_window` — granularity → (table, date column, coerced
  start, coerced end). The date-coerce step exists because Postgres casts
  ``Date`` to ``timestamptz`` using the session TZ, so passing a UTC
  ``datetime`` against a ``bucket_date`` column silently drops today's row
  whenever local-vs-UTC dates differ (see usage.py:70-73 history).
- :func:`aggregate_by_service` / :func:`aggregate_by_license` — group-and-sum
  helpers that return the routers' Pydantic DTOs directly.
- :func:`aggregate_by_provider` / :func:`aggregate_by_project` — same shape,
  for ``cost_daily``. Two axes over the SAME rows: a caller passes one filter
  list to both, so the two breakdowns of a response sum to the same total.

Filter construction stays in each route (semantics differ per endpoint).
DTO shaping stays in each route too — these helpers only own the SQL +
row→DTO step, not the response envelope.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import ColumnElement, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import CostDaily, MeteringDaily, MeteringHourly
from models.domain import (
    Granularity,
    LicenseUsage,
    ProjectCostBreakdown,
    ProviderCostBreakdown,
    ServiceUsage,
)


def granularity_window(
    start: datetime,
    end: datetime,
    granularity: Granularity,
) -> tuple[type[MeteringHourly | MeteringDaily], Any, datetime | date, datetime | date]:
    """Pick the bucket table + date column, and coerce range to match.

    Hourly buckets keep the timestamps as ``datetime`` so the WHERE clause
    compares ``bucket_hour`` against tz-aware timestamps. Daily buckets
    convert to plain ``date`` so the comparison stays inside Postgres
    ``Date`` arithmetic (no implicit Date→timestamptz cast).
    """
    if granularity == Granularity.hourly:
        return MeteringHourly, MeteringHourly.bucket_hour, start, end
    return MeteringDaily, MeteringDaily.bucket_date, start.date(), end.date()


async def aggregate_by_service(
    session: AsyncSession,
    table: type[MeteringHourly | MeteringDaily],
    filters: list[ColumnElement[bool]],
) -> list[ServiceUsage]:
    """``SUM(request_count), SUM(denied_count) GROUP BY service_key``."""
    stmt = (
        select(
            table.service_key,
            func.sum(table.request_count).label("requests"),
            func.sum(table.denied_count).label("denied"),
        )
        .where(*filters)
        .group_by(table.service_key)
    )
    rows = (await session.execute(stmt)).all()
    return [
        ServiceUsage(
            service_key=r.service_key,
            request_count=r.requests,
            denied_count=r.denied,
        )
        for r in rows
    ]


async def aggregate_by_license(
    session: AsyncSession,
    table: type[MeteringHourly | MeteringDaily],
    filters: list[ColumnElement[bool]],
) -> list[LicenseUsage]:
    """``SUM(request_count), SUM(denied_count) GROUP BY license_id``."""
    stmt = (
        select(
            table.license_id,
            func.sum(table.request_count).label("requests"),
            func.sum(table.denied_count).label("denied"),
        )
        .where(*filters)
        .group_by(table.license_id)
    )
    rows = (await session.execute(stmt)).all()
    return [
        LicenseUsage(
            license_id=str(r.license_id),
            request_count=r.requests,
            denied_count=r.denied,
        )
        for r in rows
    ]


async def aggregate_by_provider(
    session: AsyncSession,
    filters: list[ColumnElement[bool]],
) -> list[ProviderCostBreakdown]:
    """``SUM(tokens, cost, requests) GROUP BY provider`` on ``cost_daily``."""
    stmt = (
        select(
            CostDaily.provider,
            func.sum(CostDaily.total_input_tokens).label("inp"),
            func.sum(CostDaily.total_output_tokens).label("outp"),
            func.sum(CostDaily.total_cost_usd).label("cost"),
            func.sum(CostDaily.request_count).label("cnt"),
        )
        .where(*filters)
        .group_by(CostDaily.provider)
    )
    rows = (await session.execute(stmt)).all()
    return [
        ProviderCostBreakdown(
            provider=r.provider,
            input_tokens=r.inp or 0,
            output_tokens=r.outp or 0,
            cost_usd=r.cost or Decimal("0"),
            request_count=r.cnt or 0,
        )
        for r in rows
    ]


async def aggregate_by_project(
    session: AsyncSession,
    filters: list[ColumnElement[bool]],
) -> list[ProjectCostBreakdown]:
    """``SUM(tokens, cost, requests) GROUP BY (tenant_id, project_id)`` on ``cost_daily``.

    The project-axis twin of :func:`aggregate_by_provider`: same ``filters``
    argument, same summed columns, same DTO shape. Callers hand BOTH helpers the
    identical filter list, which is what makes ``by_provider`` and ``by_project``
    two decompositions of one population rather than two different populations
    that happen to sit in one response.

    ⛔ THE UUID ONLY — no name resolution here, ever. See
    :class:`models.domain.ProjectCostBreakdown` for why: a project id is
    caller-supplied and only ever a sub-dimension of an ALREADY-VERIFIED tenant,
    so a forged one can at worst move a caller's own spend between the caller's
    own projects. Resolving it to a name would be the single thing that turns it
    into a cross-tenant read. Names are fr's job, scoped by tenant there.

    ⚠️ THE SENTINEL IS A REPORTED GROUP, NOT A DROPPED ONE. ``CostDaily``
    .project_id is NOT NULL and carries ``NULL_PROJECT_ID``
    (``…-0004``) for spend no project was asserted for. That group is returned
    like any other, deliberately: filtering it out would make this breakdown sum
    to LESS than the response's ``total_cost_usd`` with nothing saying so, and
    un-attributed spend would read as absent rather than as un-attributed. The
    share it represents is the honest coverage number — the same one
    ``billing_cost_daily_project_attribution_rows`` measures by row count.

    ⛔ THE GROUP KEY IS ``(tenant_id, project_id)``, NEVER ``project_id`` ALONE.
    A project id is minted by kanban and supplied by the caller, so it is unique
    only WITHIN a tenant — nothing stops two tenants asserting the same uuid. Two
    of this helper's four call sites are operator-scope reads with NO tenant
    predicate at all (``costs.costs_by_provider``, ``costs.costs_summary``), and a
    third (``costs.costs_by_user``) applies one only when its OPTIONAL
    ``tenant_id`` query param is supplied. Grouping on the uuid alone therefore
    SUMS TWO TENANTS' SPEND INTO ONE ROW, putting one tenant's money inside
    another's breakdown with nothing in the response saying so. Qualifying the
    group by tenant makes that merge unrepresentable in the SQL, instead of
    relying on every present and future caller to remember a tenant filter.
    ``ix_cost_daily_tenant_project_date`` already leads on ``tenant_id``, so the
    wider key is the indexed column order, not a new scan shape.

    ⚠️ This is EXACTLY the condition ``billing_project_id_multi_tenant_projects``
    exists to DETECT (one project uuid arriving on two tenants' traffic). That
    gauge's rationale rests on "every read is filtered on that tenant first",
    which was NOT true of these routes; this group key is what makes it true
    here regardless of what the caller passed.

    The DTO carries ``tenant_id`` so the rows stay DISTINGUISHABLE at the edge:
    grouping correctly in SQL but emitting two rows with an identical
    ``project_id`` and nothing else to tell them apart would merely move the
    merge out of the database and into the reader.

    ⚠️ The sentinel group is UNCHANGED by this — it is now per-tenant
    ``(tenant, NULL_PROJECT_ID)`` instead of one global row, still returned, so
    the breakdown still sums to the response's ``total_cost_usd`` exactly.
    """
    stmt = (
        select(
            CostDaily.tenant_id,
            CostDaily.project_id,
            func.sum(CostDaily.total_input_tokens).label("inp"),
            func.sum(CostDaily.total_output_tokens).label("outp"),
            func.sum(CostDaily.total_cost_usd).label("cost"),
            func.sum(CostDaily.request_count).label("cnt"),
        )
        .where(*filters)
        .group_by(CostDaily.tenant_id, CostDaily.project_id)
    )
    rows = (await session.execute(stmt)).all()
    return [
        ProjectCostBreakdown(
            tenant_id=str(r.tenant_id),
            project_id=str(r.project_id),
            input_tokens=r.inp or 0,
            output_tokens=r.outp or 0,
            cost_usd=r.cost or Decimal("0"),
            request_count=r.cnt or 0,
        )
        for r in rows
    ]
