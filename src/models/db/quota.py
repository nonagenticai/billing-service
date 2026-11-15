"""Durable request-quota config — the one copy of a license's monthly request cap.

A request quota (monthly *request count* cap per license, distinct from a token
*budget*) is answered by ``GET /api/quota/check``: limit from this table, usage
from ``metering_monthly_usage``. Writers: ``POST /api/quota/config``,
``POST /api/budget/sync-from-licenses`` and the quota-config repair loop.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from db import Base

from ._common import DBUUID, _utcnow

#: Licence lifecycle statuses this mirror recognises. Deliberately the SAME four
#: the registry itself constrains to (``licenses/models.py::ck_license_status``)
#: rather than a narrowed local vocabulary: translating upstream's enum here
#: would silently coerce a status this service does not understand into one it
#: does. Only ``active`` is resolvable — see :data:`ACTIVE_LICENSE_STATUS`.
LICENSE_STATUSES: tuple[str, ...] = ("active", "revoked", "expired", "pending")

#: The single status that makes a mirror row eligible for cost attribution.
#: ``services.license_resolver`` filters on ``== ACTIVE_LICENSE_STATUS``, never
#: on ``!= 'revoked'``, so an unrecognised or newly-added upstream status fails
#: CLOSED (not resolvable) instead of quietly resolving.
ACTIVE_LICENSE_STATUS = "active"


class RequestQuotaConfig(Base):
    """Configured monthly request-quota limit for one license.

    One row per license (``license_id`` is the PK — the natural upsert target,
    unlike stacked token budgets). ``tenant_id`` is the tenant-of-record and is
    NOT NULL: ``GET /api/quota/check`` scopes a customer caller to its own
    licenses by comparing ``X-Tenant-Id`` against this column, and a row with a
    NULL tenant_id could not be proven foreign, so it was served cross-tenant to
    ANY caller (the leak closed in #63/this change). NOT NULL removes that shape
    entirely — every row now carries an owner the check can compare against.
    """

    __tablename__ = "request_quota_config"

    license_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True)
    # tenant_id is a real Postgres `uuid` column (alembic 0011). as_uuid=False is
    # deliberate: the DB type is uuid while Python keeps receiving/binding `str`, so
    # every existing comparison, log line and JSON serialisation keeps working.
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    quota_monthly: Mapped[int] = mapped_column(Integer, nullable=False)
    # WHICH APP this licence covers, mirrored from the licences service
    # (``InternalLicenseResponse.app``) by the sync-from-licenses reconcile.
    #
    # NULLABLE on purpose and it must stay that way: the app is only knowable
    # upstream, so rows written before migration 0014 -- and any licence whose
    # app the upstream omits -- carry NULL until the next reconcile. The
    # resolver treats NULL exactly like "no match" (leave license_id NULL and
    # keep counting), so an un-resolvable row degrades attribution rather than
    # failing the reconcile for every other tenant.
    #
    # Both are kept: ``app_id`` is the stable join key, ``app_name`` is what
    # operators configure ``cost_attribution_app_by_service_key`` with, because
    # a settings file full of opaque UUIDs is a mapping nobody can review.
    app_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    app_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    # The app's registry KEY (slug, e.g. ``core-app``), mirrored from
    # ``InternalLicenseResponse.app.key`` (migration 0049). It is what callers put
    # in ``X-App-Id``; ``services.license_resolver.get_app_key_map`` turns it back
    # into ``app_id``. Nullable for the same reason as ``app_id``: rows synced
    # before 0049 carry NULL until the next sync-from-licenses run.
    app_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    # LICENCE LIFECYCLE, mirrored from ``InternalLicenseResponse.status``
    # (migration 0025). Before this column existed there was no lifecycle field
    # on this table AT ALL, and ``services.license_resolver`` documented the
    # consequence: a revoked licence stayed resolvable and its tenant kept
    # attracting attributed invoice lines until a reconcile happened to delete
    # the row. Measured 2026-09-08: 43 unrevoked, unexpired tokens hung off
    # revoked licences.
    #
    # NOT NULL with a server default of 'active' so the column is safe to add to
    # a live table and every pre-existing row keeps resolving exactly as before
    # (see 0025's BACKFILL note — any other backfill would silently stop
    # attributing real spend on deploy).
    #
    # ⚠️ This is upstream's STORED status, not its derived
    # ``effective_status()``: ``internal_list_licenses`` returns ``lic.status``
    # verbatim. A licence past ``expires_at`` whose stored status is still
    # 'active' therefore mirrors as 'active'. Closing THAT needs ``expires_at``
    # mirrored as well; this column does not claim to be a complete lifecycle
    # filter, only the revocation half.
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=ACTIVE_LICENSE_STATUS
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    # ``quota_monthly >= 0`` — a configured **0 means block-all** (a zero-allowance
    # / suspended plan), NOT unlimited. "Unlimited" is the ABSENCE of a row / a
    # NULL limit (``check_quota`` returns ``over_quota=False`` only when the limit
    # is ``None``). Negatives are still rejected. The constraint was ``> 0`` until
    # a 0 became a first-class block-all value; renamed from
    # ``ck_request_quota_positive`` (migration 0008).
    __table_args__ = (
        CheckConstraint("quota_monthly >= 0", name="ck_request_quota_non_negative"),
        # Mirrors licences' own ck_license_status. Kept in lockstep with
        # LICENSE_STATUSES so the mirror can never hold a value the registry
        # itself would reject.
        CheckConstraint(
            "status IN ('active', 'revoked', 'expired', 'pending')",
            name="ck_request_quota_status",
        ),
    )
