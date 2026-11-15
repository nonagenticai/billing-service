"""The licences APP registry, mirrored — ``app_key -> app_id`` for X-App-Id.

Every inference-router caller sends its app KEY (``core-app``,
``chat-bridge-rmt``) in ``X-App-Id``; ``services.license_resolver`` turns it
back into the registry app uuid. That map used to be built from
``request_quota_config.app_key``, which only holds apps that own a LICENCE —
so platform apps registered by licences' ``scripts/seed_apps.py`` with no
licence (``core-app``, ``chat-bridge*``) never resolved and their spend was
stored with ``app_id`` NULL.

This table mirrors the WHOLE registry (``GET {licenses_url}/internal/apps``,
revoked apps included and flagged), refreshed by
``POST /api/budget/sync-from-licenses`` (migration 0050).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from db import Base

from ._common import DBUUID, _utcnow


class AppRegistry(Base):
    """One row per app registered in licences (revoked ones flagged, not dropped)."""

    __tablename__ = "app_registry"

    app_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True)
    app_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    # A revoked app still owns its historical spend, so its key keeps resolving;
    # the flag is mirrored so the fact is visible, not so the map can drop it.
    revoked: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("now()")
    )
