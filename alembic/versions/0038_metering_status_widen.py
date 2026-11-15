"""billing_metering_events.status — widen 20 -> 64, real vocabulary was truncating.

THE DEFECT
----------
``status`` was ``String(20)``. Any wire value longer than that could not be
stored at all: ``services.event_flusher._bound_status_for_storage`` swaps it
for the literal sentinel ``"_too_long"`` before the INSERT, and only a 32-char
prefix survives, in a ``logger.warning``. Measured live 2026-09-16: **1,509
rows/24h** carry ``_too_long`` — the true status of 1,509 events per day is
unrecoverable from the database. All observed occurrences in the last 48h are
``"allowed_service_principal"`` (25 chars), license-authz's own status for a
service-principal-authenticated call (``license-authz/src/main.py:1462``);
this population is currently billable either way (see below), so today's loss
is purely observability, not money.

⛔ BUT THE 20-CHAR BOUND WAS ALSO A LATENT BILLING-INTEGRITY BUG, NOT JUST AN
OBSERVABILITY ONE. ``license-authz/src/main.py`` also emits
``"denied_service_principal"`` (24 chars) and ``"denied_invalid_tenant"``
(21 chars) — both DENIED outcomes, both already over the old 20-char limit.
Had either ever hit this path, it would have been stored as ``_too_long``,
which does NOT start with ``"denied"``
(``services.metering_status.is_denied_status``), so a genuinely denied request
would have been billed as if it were allowed. Not observed in the sampled
window, but reachable by code already shipped in license-authz today.

WHY 64, NOT UNBOUNDED
----------------------
64 mirrors the sibling ``service_key`` column's own bound
(``models/db/events.py``) and gives 2.5x headroom over the longest status
literal measured live (``allowed_service_principal``, 25 chars) and over every
denial reason enumerated in license-authz's source. The column stays bounded,
not ``Text``, because the failure mode the OLD bound was defending against is
real and unrelated to this widening: an emitter can put an exception message
where a status was expected (measured:
``"ValueError: connection reset by peer at ...req=abc123"``, 62 chars) with no
attacker required. ``services.event_flusher._STATUS_TOO_LONG`` and its
``"_too_long"`` sentinel are UNCHANGED in shape and in the safety property that
matters (the sentinel itself can never start with ``"denied"`` or equal
``"allowed"``) -- only the threshold moved, from 20 to 64
(``_STATUS_COLUMN_MAX``), to match this column.

CLASSIFICATION IMPACT -- STATED EXPLICITLY
-------------------------------------------
``services.metering_status.is_denied_status`` classifies on
``status.startswith("denied")`` and is unaffected by column width. For every
row actually observed in the live sample (all ``"allowed_service_principal"``,
never denied-prefixed either as the full value or as ``_too_long``),
classification does NOT change: billable before, billable after. For the two
theoretical-but-shipped denial reasons above (24 and 21 chars), classification
DOES change, and correctly: today those would misclassify as billable via the
``_too_long`` escape hatch; after this migration they are stored in full and
correctly classify as denied. This is a bug fix, not new risk -- the sentinel
still cannot itself be mistaken for either vocabulary.

NO ADDITIVE_MIGRATIONS ENTRY
------------------------------
Unlike 0032/0036/0037's ``ADD COLUMN`` entries in ``src/db.py``, a plain
``ALTER COLUMN ... TYPE`` widening carries none of that ordering hazard: OLD
app code never writes more than 20 chars regardless of the column's width, so
a pod running pre-0038 code against a post-0038 schema is unaffected, and each
pod's own ``migrate`` init container applies this migration before that pod's
NEW code (which can write up to 64) ever starts. There is no window where new
code can outrun the schema.

Revision ID: 0038_metering_status_widen
Revises: 0037_billed_while_archived
Create Date: 2026-09-16
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0038_metering_status_widen"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0037_billed_while_archived"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "billing_metering_events"
_COLUMN = "status"
_OLD_LEN = 20
_NEW_LEN = 64


def upgrade() -> None:
    op.alter_column(
        _TABLE,
        _COLUMN,
        existing_type=sa.String(length=_OLD_LEN),
        type_=sa.String(length=_NEW_LEN),
        existing_nullable=False,
    )


def downgrade() -> None:
    # Lossy as data, same trade-off 0032/0037 accept for their own columns: any
    # value written between 21 and 64 chars while at width 64 does not fit back
    # into 20 and Postgres will refuse the narrowing ALTER outright
    # (`StringDataRightTruncation`) rather than silently truncate it. Truncate
    # explicitly first so the downgrade is at least deterministic, matching
    # `_bound_status_for_storage`'s own sentinel rather than an arbitrary cut.
    op.execute(f"UPDATE {_TABLE} SET {_COLUMN} = '_too_long' WHERE length({_COLUMN}) > {_OLD_LEN}")
    op.alter_column(
        _TABLE,
        _COLUMN,
        existing_type=sa.String(length=_NEW_LEN),
        type_=sa.String(length=_OLD_LEN),
        existing_nullable=False,
    )
