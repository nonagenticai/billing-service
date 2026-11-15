"""``tenant_id`` becomes a real ``uuid`` column on all 14 tenant-bearing tables.

Fleet lane "tenant_id is ALWAYS a uuid", service #3 of 46. Predecessors, both
shipped and proven: ``requirements_management`` ``2026_08_24_0042`` (23 tables)
and ``worker_management`` ``024`` (12 tables). The UUIDv5 namespace below is
byte-identical to the one both of them used, so the same legacy label derives
the same uuid fleet-wide.

WHAT IS BROKEN
--------------
``billing`` stores ``tenant_id`` as ``varchar(255)`` and therefore stores
whatever it is handed. Measured live against the prod billing primary
(db ``billing``) on 2026-08-25: **35,890 rows over
14 tables carry a non-uuid ``tenant_id``, across 32 distinct literals.**

The defect this closes is not aesthetic. One tenant is currently split across
two identifiers *in the same table*: ``token_usage`` holds two rows for one
(licence, period) — one under the label ``n8n-dev`` and one under
``019de842-c92a-7e37-8fdd-eb0d8c5396f9``, which IS ``n8n-dev``. Usage is billed
to two identities and reachable from neither. Same shape in
``billing_metering_daily`` (``''`` and ``unknown`` for one licence-day).

The 30 non-credential literals and their live row counts (the counts drift
daily — ``unknown`` and ``__unattributed__`` keep arriving; see the merge note
below, which is exactly why the merge is COMPUTED, not frozen)::

    __unattributed__            20342   |  probe-sales                    4
    unknown                     14805   |  costbus-verify-probe           4
    n8n-dev                       411   |  judge-tier-proof-20260625      4
    '' (empty string)             106   |  kag                            4
    gerard-dev                     63   |  p5-meter-test                  4
    portal                         38   |  probe-persisted-e2e            4
    default                        33   |  probe-test-tenant              4
    probe                          13   |  s11-e2e-verify                 4
    e2e-smoke-xtzz3028              9   |  subhankar                      4
    fr-test                         9   |  verify-tenant                  4
    proof-tier-diff-20260624        7   |  x                              4
    probe-tenant                    7   |  y                              4
    n8n-test                        5   |  probe-test                     4
    autonomy-verify-probe           3   |  zzz-no-grant-verify-tenant     3
    e2e-smoke-qe95guoq              3   |  verify-probe-0d996e70          2

Plus a CREDENTIAL LEAK: 4 ``token_usage`` rows hold a raw ``Authorization``
header in ``tenant_id`` — and the same string is duplicated into
``tenant_name``. **Neither the tokens nor any recoverable form of them appears
in this file.** They are matched by SHAPE, mirroring
``src/services/_parsers.py::_looks_like_credential``, mapped to the nil tenant,
and their ``tenant_name`` is overwritten with ``__unattributed__``. The
crosswalk row that records them carries ``'bearer:<first 12 hex of sha256>'``,
computed inside Postgres so the token never enters this process — a committed
literal would be a NEW leak and ``detect-secrets`` (pre-commit,
``.secrets.baseline``) would reject the commit anyway.

THE MAPPING (auditable, and recorded in ``tenant_id_crosswalk`` before a single
row is touched)::

    exact name in licenses.tenants      -> that registry uuid    source='registry'
    '__unattributed__' | 'unknown' | '' -> 00000000-…-0          source='sentinel'
    '__default__'                       -> uuid5(NS,'__default__') source='sentinel'
    looks like a credential             -> 00000000-…-0          source='credential'
                                           (+ tenant_name scrubbed)
    anything else                       -> uuid5(NS, label)      source='derived'

v5 and not v7 on purpose: a v7 encodes a mint time, and there is no honest mint
time for a value that was already sitting in the table. A genuine v7 can only
come from registering the tenant in ``licenses`` — an owner decision, not a
migration's.

WHY THE REGISTRY UUID FOR ``n8n-dev`` AND NOT ``worker_management``'s DERIVED ONE
--------------------------------------------------------------------------------
``X-Tenant-Id`` is minted at ``licenses/models.py:3482`` as
``str(license.tenant_id)``, a NOT NULL FK to ``licenses.tenants.id``;
``license-authz`` forwards it verbatim; 225/225 licences join. Measured inside
billing itself: the registry uuid ``019de842-c92a-…`` ALREADY carries 44,712
``billing_cost_events`` and 301,917 ``billing_metering_events`` rows, while the
uuid5-derived ``82d552da-…`` that ``worker_management`` uses carries **0 rows
here**. So billing maps the label ``n8n-dev`` onto the registry uuid. That is a
fact about this database, not a fleet-wide re-decision — ``worker_management``'s
derived choice is deliberate and governed (its migration 024 documents measured
unique-constraint collisions on both merge candidates, and gitops
``keycloak/realm-patch-script-configmap.yaml`` carries an explicit "do NOT fix
this by renumbering" directive). Two auth paths legitimately carry two
identifiers and billing receives both.

WHY THE NIL UUID IS NOT AN INVENTION
------------------------------------
``00000000-0000-0000-0000-000000000000`` is a REGISTERED tenant in
``licenses.tenants`` (name ``default-anon``), and it is already the value
emitters send to mean "no tenant" (``docs/EMITTER_GUIDE.md:78-86``).
``_normalize_tenant_id`` turns exactly that value INTO ``'__unattributed__'``
today, so mapping the sentinels back onto it is a round trip.

``'__default__'`` IS RESERVED EVEN THOUGH IT IS NOT IN THE DATA
---------------------------------------------------------------
``src/routers/config.py:341`` writes the literal ``"__default__"`` into
``TenantConfig.tenant_id`` and ``src/services/billing.py:50,89,93`` +
``routers/config.py:310,331`` read it back. ``tenant_config`` holds 0 rows
today, so there is nothing to remap — but the column becomes ``uuid`` and the
NEXT write of that literal would fail. Its crosswalk row therefore exists for
the CODE, not for the data, and the coverage guard below will never see it.
``src/tenancy.py::DEFAULT_CONFIG_TENANT_ID`` must equal
``uuid5(TENANT_NAMESPACE, "__default__")`` — which is why this file DERIVES it
rather than pasting a literal: two hand-copied spellings of the same uuid is a
defect waiting for a typo.

Deliberate divergence, recorded so it is not read as an oversight:
``markup_rules`` solved the same problem the other way — ``src/models/db/
config.py:105-106`` says NULL is the wildcard and "supersedes the legacy
'__default__' string sentinel". ``tenant_config.tenant_id`` is NOT NULL and
UNIQUE, so making it nullable would mean dropping and rebuilding the uniqueness
of the whole config table; a reserved uuid is the lower-blast-radius choice
there. Both are correct for their own constraint shape.

``'__admin__'`` IS DELIBERATELY NOT MINTED
------------------------------------------
``src/auth_deps.py:168`` can return ``"__admin__"`` and
``src/routers/webhooks.py:71`` INSERTs whatever it returns into
``tenant_webhooks.tenant_id``. Measured: ``__admin__`` is NOT among the 32
distinct non-uuid literals live in this database — that write path has never
fired. This migration therefore does NOT mint a tenant for it. Minting one
would make an admin JWT look like a tenant forever. The fix is at the boundary
(``require_tenant``, shipped by the same PR): an admin call must not fabricate a
tenant identity at all. If ``__admin__`` ever DOES appear in the corpus before
this runs, the total-coverage guard below RAISEs and names it rather than
silently deriving a uuid for it. That is the intended outcome.

WHAT IS OUT OF SCOPE (measured, not overlooked)
-----------------------------------------------
* **The 35,228 sentinel rows are UNRECOVERABLE, not a backlog.** Only 4 of the
  20,192 ``__unattributed__`` rows carry a ``license_id`` at all, and that
  licence does not exist in ``licenses``. There is no crosswalk to a real
  tenant and this file does not pretend there is — they go to the nil tenant,
  which is what they already meant. The producer gap (inference-router
  east-west traffic bypassing the ingress ext_authz, so no ``X-Tenant-Id`` is
  attached — ONGOING, ~146 rows/day) is a different lane.
* **``tenant_name`` stays ``varchar``.** It is a real display name, not a copy:
  ``token_budgets`` has 3 rows where it is a human slug over a uuid
  ``tenant_id``, and ``audit_log`` has 155 where it is blank over one. Only the
  4 credential rows have it overwritten, because there it is a leaked secret —
  plus a collision-merge survivor, which takes the best label its OWN group
  already held (see guard 3); nothing is invented and no other row changes.
* **The other 43 databases.** One service per session.

WHY EACH GUARD EXISTS (FAIL LOUD ON DIVERGENCE, NO-OP ON ABSENCE)
------------------------------------------------------------------
Every guard here has a zero-arm that is a clean no-op, because ``alembic
upgrade head`` MUST succeed against an EMPTY database: CI runs it against an
ephemeral ``postgres:18`` container before any test, and billing's ``migrate``
init container would CrashLoop forever on any fresh, staging or DR-restored DB.
The zero-arm is load-bearing, not a loophole — the same shape as
``0007_quota_tenant_not_null`` and ``0010_license_sync_tenant_repair``.

1. **Total-coverage guard.** Collect every distinct non-uuid literal actually
   live across the tables this migration will actually rewrite — the OWNED
   ones, minus credential-shaped values, handled by pattern — and RAISE naming
   any the crosswalk does not cover. It deliberately runs AFTER the ownership
   guard: a literal sitting in a table this role cannot ``ALTER`` must not
   abort a migration that was never going to touch that table. Without it a
   literal that arrived between authoring and deploy dies at the ALTER instead
   — *after* other tables have already been rewritten. It uses
   ``pg_input_is_valid``, so this migration requires **PostgreSQL >= 16** and
   asserts that explicitly rather than dying with an opaque
   ``UndefinedFunction`` on an older restore.
2. **Ownership guard.** ``pg_has_role(current_user, relowner, 'USAGE')`` is the
   exact privilege ``ALTER TABLE`` requires. A table this role does not own is
   skipped and NAMED on stdout, never taken down with the deployment. It runs
   BEFORE every other step, and everything downstream — the coverage guard, the
   merges, the remap, the ``markup_rules`` index DROP/CREATE and the ALTERs —
   is gated on its result. The index DDL is not an exception: ``DROP INDEX`` on
   a table this role does not own fails with ``must be owner of index`` and
   would abort the whole transaction seconds after the guard printed
   ``SKIP not owner`` for that very table.
   ``requirements_management`` took a **17-minute production outage** on exactly
   this shape: an ad-hoc snapshot table owned by ``postgres``, ``must be owner
   of table``, CrashLoopBackOff, and a ``Recreate`` strategy that had already
   deleted the old pod. Measured here 2026-08-25: ``billing_user`` owns all 33
   public tables, so this guard is expected to skip nothing — which is precisely
   when a missing guard is cheapest to omit and most expensive to have omitted.
   Reported with ``print(..., flush=True)`` and never ``logger.warning``: a
   warning from inside an alembic migration is swallowed by alembic's logging
   config, and an init container's stdout is the one channel an operator reads.
3. **Merge of remap collisions, COMPUTED AT RUNTIME.** Collapsing two spellings
   onto one uuid can violate ``uq_hourly`` / ``uq_daily`` / ``uq_cost_daily`` /
   ``uq_token_usage`` / ``tenant_config_tenant_id_key`` / ``ix_accesscharge_open``
   / ``ix_markuprules_open``. Pre-flighted live 2026-08-25 against this exact
   mapping: ``uq_daily`` 1 group, ``uq_token_usage`` 5 groups, everything else
   0. **That set WILL be different at deploy time** — ``unknown`` and
   ``__unattributed__`` rows arrive daily — so a frozen row list would be stale
   before it shipped. The four tables with additive counters sum them into the
   lowest ``id`` and delete the rest; the three with no additive column
   (``tenant_config``, ``tenant_access_charge``, ``markup_rules``) RAISE, because
   silently dropping one of two rows there loses configuration, not a counter.
   Where the merged table carries ``tenant_name``, the survivor's is PINNED to
   the group's best available human label: any value that is not NULL, not
   ``''``, not ``unknown``, not ``__unattributed__`` and not credential-shaped
   wins, remaining ties broken by the same ``ORDER BY id``. Without that rule
   the surviving display name is "whichever row happened to be inserted first",
   which is neither meaningful nor reproducible in a test.
4. **Postcondition.** Re-read ``information_schema`` and RAISE naming any of the
   14 still not ``uuid``. Without it, "flipped" and "silently skipped by the
   ownership guard" are indistinguishable from a green log line.

INDEX AND PARTITION LANDMINES
------------------------------
* ``markup_rules.ix_markuprules_lookup`` and ``ix_markuprules_open`` both index
  ``COALESCE(tenant_id, ''::character varying)``. ``coalesce(uuid, '')`` has NO
  type resolution in Postgres, so both MUST be dropped before the ALTER and
  recreated as ``COALESCE(tenant_id::text, '')``. That is not an invention: the
  same index already does exactly this for ``license_id``
  (``src/models/db/config.py:141``) — this follows the file's own precedent.
* ``billing_cost_events`` is RANGE-partitioned into 13 monthly children
  (2026_05..2027_05, no DEFAULT partition, no sub-partitioning; 426,899 rows).
  ``ALTER TABLE`` on the PARENT recurses and rebuilds all 26 child tenant
  indexes. Children are never altered individually — doing so would leave the
  parent and its partitions with mismatched column types.
* Biggest rewrite is ``billing_metering_events`` at 480 MB / 741,913 rows.
  ``SET LOCAL lock_timeout = '10s'`` is the FIRST statement executed by both
  ``upgrade()`` and ``downgrade()``, before any catalog read or DDL, so EVERY
  lock this migration takes is bounded — the merge UPDATE/DELETEs, the remap
  UPDATEs and the index DROPs included, not just the ALTERs. 10s and not 90s
  because the whole migration is one transaction (``alembic/env.py:75-76``
  wraps it): a lock failure rolls the entire thing back and costs only a retry,
  whereas a statement that sits waiting on a live pod's lock blocks every
  reader queued behind it for as long as the timeout allows. Real API traffic
  here is ~1 request/hour (the rest is health probes) and there are 8 active
  backends, so the rewrite itself, once the lock is held, is cheap.

NULL SEMANTICS IN THE MERGE — WHICH ONE AND WHY
------------------------------------------------
The merge groups with SQL ``GROUP BY``, which is ``IS NOT DISTINCT FROM``
semantics: two NULL-keyed rows land in the same group. It then refuses to merge
any group whose key parts are not all NOT NULL. Both halves are deliberate.

Verified against the live definitions rather than assumed: ``uq_hourly``,
``uq_daily``, ``uq_cost_daily`` and ``uq_token_usage`` are plain
``UNIQUE`` constraints (``src/db.py:183-192``, ``src/models/db/rollups.py``,
``src/models/db/budgets.py:129``) — **no ``NULLS NOT DISTINCT`` anywhere in this
repo**, so Postgres' default applies and two rows with a NULL key part do NOT
collide. Merging them would therefore destroy rows the database is perfectly
happy to keep. The NOT NULL condition makes the merge collapse *exactly* the
groups that would break the unique index rebuild during the ALTER, and nothing
more. In a schema-conformant database the condition is a pure no-op anyway:
``license_id``/``subject_id``/``app_id``/``plan_id`` are all NOT NULL with
sentinel-uuid defaults (``NULL_LICENSE_ID`` etc.), so no such group can exist —
it is there for a drifted environment, and it fails toward keeping data.

DOWNGRADE
---------
``downgrade()`` reverses the column TYPE only. The crosswalk table and the
merged rows STAY: a merge folded two rows that were always one tenant, and
nothing in the surviving row records which counter came from which spelling, so
re-splitting them is not possible — only guessable. Read
``tenant_id_crosswalk`` if you need the mapping after a downgrade.

Revision ID: 0011_tenant_id_is_a_uuid
Revises: 0010_license_sync_tenant_repair
Create Date: 2026-08-25

(Revision id kept <=32 chars — ``alembic_version.version_num`` is varchar(32).)
"""

from __future__ import annotations

import uuid

import sqlalchemy as sa
from alembic import op

revision = "0011_tenant_id_is_a_uuid"
down_revision = "0010_license_sync_tenant_repair"
branch_labels = None
depends_on = None

CROSSWALK = "tenant_id_crosswalk"

# Byte-identical to requirements_management 2026_08_24_0042 and
# worker_management 024. NEVER change it: the same legacy label must derive the
# same uuid in every service, and in tests.
TENANT_NAMESPACE = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")

# A REGISTERED tenant (licenses.tenants name 'default-anon'), and already what
# emitters send for "no tenant". Not an invented sentinel.
NIL = "00000000-0000-0000-0000-000000000000"

UNATTRIBUTED = "__unattributed__"

# Exact-name matches in licenses.tenants for the literals billing actually
# holds. Hardcoded rather than looked up: a migration must not depend on a
# network call, and these pairings are facts about data that already exists.
REGISTRY: dict[str, str] = {
    "n8n-dev": "019de842-c92a-7e37-8fdd-eb0d8c5396f9",
    "n8n-test": "019de842-c92b-752e-b39c-30d64950e1cc",
    "portal": "019de842-c92b-7564-b932-98240d274b3b",
    "fr-test": "019efaa0-512b-733e-9e98-b1ac82a6f7ee",
    "e2e-smoke-xtzz3028": "019de842-c92b-75b0-9502-88a436832a67",
    "e2e-smoke-qe95guoq": "019de842-c92b-7579-a7e6-5b79642d6c80",
}

# "No tenant" spellings. All three collapse onto the nil tenant.
SENTINELS = (UNATTRIBUTED, "unknown", "")

# A config SCOPE, not a tenant — see the docstring. Present for the code path
# (routers/config.py:341), absent from the data, so the coverage guard never
# sees it. src/tenancy.py::DEFAULT_CONFIG_TENANT_ID must derive identically.
RESERVED_SCOPES = ("__default__",)

# The 30 non-credential literals measured live 2026-08-25. Counts are in the
# docstring; the set is what matters here. The two credential-shaped values are
# DELIBERATELY absent — they are matched by shape (CREDENTIAL_PRED) and never
# written into this file.
LEGACY_VALUES: tuple[str, ...] = (
    UNATTRIBUTED,
    "unknown",
    "n8n-dev",
    "",
    "gerard-dev",
    "portal",
    "default",
    "probe",
    "e2e-smoke-xtzz3028",
    "fr-test",
    "proof-tier-diff-20260624",
    "probe-tenant",
    "n8n-test",
    "probe-sales",
    "costbus-verify-probe",
    "judge-tier-proof-20260625",
    "kag",
    "p5-meter-test",
    "probe-persisted-e2e",
    "probe-test-tenant",
    "s11-e2e-verify",
    "subhankar",
    "verify-tenant",
    "x",
    "y",
    "probe-test",
    "autonomy-verify-probe",
    "zzz-no-grant-verify-tenant",
    "e2e-smoke-qe95guoq",
    "verify-probe-0d996e70",
)

# The 14 tenant-bearing tables. Explicit rather than discovered, so a table
# added later cannot be silently swept into a type change nobody reviewed.
# billing_cost_events is the RANGE-partitioned parent — its 13 children are
# NOT listed and MUST NOT be altered individually; the parent ALTER recurses.
TENANT_TABLES: tuple[str, ...] = (
    "audit_log",
    "billing_cost_daily",
    "billing_cost_events",
    "billing_metering_daily",
    "billing_metering_events",
    "billing_metering_hourly",
    "budget_overage_alerts",
    "markup_rules",
    "request_quota_config",
    "tenant_access_charge",
    "tenant_config",
    "tenant_webhooks",
    "token_budgets",
    "token_usage",
)

# Tables where a post-remap duplicate is MERGEABLE: the unique key minus
# tenant_id, plus the columns that are counters and may be summed. The additive
# list is asserted against information_schema before it is used, so it can never
# silently drift from the schema (a summed column that quietly stopped existing
# would be a wrong invoice, not an error).
MERGE_TABLES: dict[str, dict[str, tuple[str, ...]]] = {
    "billing_metering_hourly": {
        "key": ("license_id", "subject_id", "app_id", "plan_id", "service_key", "bucket_hour"),
        "additive": ("request_count", "denied_count", "budget_denied_count"),
    },
    "billing_metering_daily": {
        "key": ("license_id", "subject_id", "app_id", "plan_id", "service_key", "bucket_date"),
        "additive": ("request_count", "denied_count", "budget_denied_count"),
    },
    "billing_cost_daily": {
        "key": (
            "license_id",
            "subject_id",
            "service_key",
            "provider",
            "served_model",
            "byo",
            "owner_provided",
            "cost_source",
            "bucket_date",
        ),
        "additive": (
            "total_input_tokens",
            "total_output_tokens",
            "total_cost_usd",
            "request_count",
        ),
    },
    "token_usage": {
        "key": ("license_id", "period_type", "period_start"),
        "additive": ("input_tokens", "output_tokens", "total_tokens"),
    },
}

# Tables where a post-remap duplicate is NOT mergeable — there is no counter to
# sum, only configuration, and picking a survivor is an operator decision.
# Key expressions mirror the LIVE unique index of each table exactly, including
# its coalesce()s and its partial WHERE.
NO_MERGE_KEYS: dict[str, tuple[str, ...]] = {
    # tenant_config_tenant_id_key: UNIQUE (tenant_id)
    "tenant_config": ("r.new_tenant_id",),
    # ix_accesscharge_open: UNIQUE (tenant_id, coalesce(service_key,''))
    #                       WHERE effective_until IS NULL
    "tenant_access_charge": ("r.new_tenant_id", "coalesce(r.service_key, '')"),
    # ix_markuprules_open: UNIQUE (coalesce(tenant_id,''), coalesce(license_id::text,''),
    #                              coalesce(service_key,''), coalesce(provider,''),
    #                              coalesce(model,'')) WHERE effective_until IS NULL
    # tenant_id is NULLABLE here (NULL = the wildcard/global rule), so the
    # coalesce is load-bearing and a NULL must never be remapped.
    "markup_rules": (
        # ::text is load-bearing. `new_tenant_id` is a uuid once the remap
        # expression casts (it did not always), and `coalesce(uuid, '')` has NO
        # type resolution in Postgres — it fails at PARSE time, so this kills the
        # guard even on an empty table. Exactly the landmine this migration's own
        # docstring documents for the markup_rules INDEXES, one level down.
        # `license_id` on the next line already had to do this; tenant_id did not
        # until it became a uuid.
        "coalesce(r.new_tenant_id::text, '')",
        "coalesce(r.license_id::text, '')",
        "coalesce(r.service_key, '')",
        "coalesce(r.provider, '')",
        "coalesce(r.model, '')",
    ),
}

# The partial-index predicate each of those uniques carries, if any.
NO_MERGE_WHERE: dict[str, str] = {
    "tenant_config": "",
    "tenant_access_charge": "WHERE t.effective_until IS NULL",
    "markup_rules": "WHERE t.effective_until IS NULL",
}

# Mirrors src/services/_parsers.py::_looks_like_credential. Deliberately
# slightly NARROWER than the Python (which tests a bare 'bearer' prefix with no
# following space): a tenant legitimately named "bearerish" is not a secret, and
# any real header is caught anyway by length > 48 or by the whitespace class.
# Anything this predicate lets through that is ALSO not in the crosswalk is
# named by the total-coverage guard rather than silently mapped — the narrowing
# fails loud, not open.
_CRED = (
    "({col} ILIKE 'bearer %' OR {col} ILIKE 'basic %' OR {col} ILIKE 'token %' "
    "OR length({col}) > 48 OR {col} ~ '[[:space:]]')"
)

MARKUP_LOOKUP = "ix_markuprules_lookup"
MARKUP_OPEN = "ix_markuprules_open"


# The label-ranking predicate. `_CRED` deliberately catches whitespace and
# length > 48 because a tenant *id* never contains either — but a display NAME
# legitimately does ("Acme Industries"), and ranking that equal to 'unknown'
# would let a slug beat a real name for the surviving row. So the label ranking
# tests only the header prefixes.
_CRED_NAME = "({col} ILIKE 'bearer %' OR {col} ILIKE 'basic %' OR {col} ILIKE 'token %')"


def _cred_name(col: str) -> str:
    """The credential-shape predicate for a DISPLAY NAME (see ``_CRED_NAME``)."""
    return _CRED_NAME.format(col=col)


def _cred(col: str) -> str:
    """The credential-shape predicate applied to ``col``."""
    return _CRED.format(col=col)


def canonical_tenant_uuid(legacy: str) -> tuple[str, str]:
    """``(uuid, source)`` for a legacy tenant value. Pure — unit-testable.

    Credential-shaped values never reach this function: they are matched by
    shape in SQL so the token never enters this process.
    """
    if legacy in REGISTRY:
        return REGISTRY[legacy], "registry"
    if legacy in SENTINELS:
        return NIL, "sentinel"
    if legacy in RESERVED_SCOPES:
        return str(uuid.uuid5(TENANT_NAMESPACE, legacy)), "sentinel"
    return str(uuid.uuid5(TENANT_NAMESPACE, legacy)), "derived"


_NOTES = {
    "registry": "exact name match in licenses.tenants; the uuid licence-token traffic already sends",
    "sentinel": "not a tenant — a 'no tenant' / config-scope spelling; see migration 0011 docstring",
    "derived": (
        "no licences tenant resolves this label; deterministic UUIDv5 of the label, 1:1 so the "
        "isolation boundary is unchanged and it can be re-pointed at a real tenant later"
    ),
    "credential": (
        "a leaked auth header found in tenant_id; remapped to the nil tenant and tenant_name "
        "scrubbed. legacy_value is a sha256 prefix — the token is deliberately NOT recorded"
    ),
}


# --------------------------------------------------------------------------
# Live-schema readers. Everything is read from information_schema / pg_class,
# never from Base.metadata: the models ship FLIPPED to Uuid in the same PR, so
# metadata can no longer describe what the database currently holds, and a
# migration that renders DDL from live model types breaks retroactively the
# next time a model changes.
# --------------------------------------------------------------------------


def _tenant_columns(conn: sa.Connection) -> dict[str, str]:
    """``{table: data_type}`` for the tenant tables that actually exist here.

    A fresh CI database is built by ``alembic upgrade head`` alone, which does
    NOT create every table the ORM knows about (``tenant_access_charge`` has no
    migration — it comes from ``db.py``'s bootstrap). Absent tables are simply
    not in this mapping, which makes every step below a no-op for them.
    """
    rows = conn.execute(
        sa.text(
            "SELECT table_name, data_type FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND column_name = 'tenant_id' "
            "  AND table_name = ANY(:names) ORDER BY 1"
        ),
        {"names": list(TENANT_TABLES)},
    )
    return {r[0]: r[1] for r in rows}


def _columns(conn: sa.Connection, table: str) -> set[str]:
    rows = conn.execute(
        sa.text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = :t"
        ),
        {"t": table},
    )
    return {r[0] for r in rows}


def _owned(conn: sa.Connection, tables: list[str]) -> list[str]:
    """Of ``tables``, the ones this role may actually ``ALTER``.

    ``pg_has_role(…, 'USAGE')`` is the exact privilege ``ALTER TABLE`` requires:
    be the owner, or be a member of the owning role. Anything else is skipped
    and NAMED, rather than taking the deployment down with it — see the
    17-minute-outage note in the module docstring.
    """
    if not tables:
        return []
    rows = conn.execute(
        sa.text(
            "SELECT c.relname, pg_has_role(current_user, c.relowner, 'USAGE') "
            "FROM pg_class c WHERE c.relnamespace = current_schema()::regnamespace "
            "  AND c.relkind IN ('r', 'p') AND c.relname = ANY(:names) ORDER BY 1"
        ),
        {"names": tables},
    )
    owned: list[str] = []
    for name, may_alter in rows:
        if may_alter:
            owned.append(name)
        else:
            # print(), not logger.warning(): a warning from inside an alembic
            # migration is swallowed, and an init container's stdout is the one
            # channel an operator will actually read.
            print("SKIP not owner:", name, flush=True)
    return owned


# --------------------------------------------------------------------------
# Steps
# --------------------------------------------------------------------------


def _create_crosswalk() -> None:
    """Step 1a — the audit table. Idempotent, and safe to keep after downgrade."""
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CROSSWALK} (
            legacy_value text PRIMARY KEY,
            tenant_uuid  uuid NOT NULL,
            source       text NOT NULL,
            note         text,
            created_at   timestamptz NOT NULL DEFAULT now()
        )
        """
    )


def _seed_crosswalk(conn: sa.Connection) -> None:
    """Step 1b — record the mapping BEFORE a single row is touched.

    ON CONFLICT DO NOTHING: a re-run must not rewrite an operator's later
    correction of a mapping.
    """
    for value in sorted(set(LEGACY_VALUES) | set(RESERVED_SCOPES)):
        mapped, source = canonical_tenant_uuid(value)
        conn.execute(
            sa.text(
                f"INSERT INTO {CROSSWALK} (legacy_value, tenant_uuid, source, note) "
                "VALUES (:v, CAST(:u AS uuid), :s, :n) "
                "ON CONFLICT (legacy_value) DO NOTHING"
            ),
            {"v": value, "u": mapped, "s": source, "n": _NOTES[source]},
        )


def _record_credentials(conn: sa.Connection, text_tables: list[str]) -> None:
    """Step 1c — one REDACTED crosswalk row per credential-shaped value found live.

    The sha256 is computed inside Postgres and truncated to 12 hex characters:
    the token never enters this process, never reaches a log, and the recorded
    value is not reversible. It exists only so an auditor can see that N
    distinct leaked credentials were scrubbed, and correlate two occurrences of
    the same one.
    """
    for table in text_tables:
        conn.execute(
            sa.text(
                f"INSERT INTO {CROSSWALK} (legacy_value, tenant_uuid, source, note) "
                "SELECT DISTINCT "
                "  'bearer:' || left(encode(sha256(convert_to(t.tenant_id, 'UTF8')), 'hex'), 12), "
                f"  CAST('{NIL}' AS uuid), 'credential', :n "
                f"FROM {table} t WHERE {_cred('t.tenant_id')} "
                "ON CONFLICT (legacy_value) DO NOTHING"
            ),
            {"n": _NOTES["credential"]},
        )


def _coverage_guard(conn: sa.Connection, text_tables: list[str]) -> None:
    """Step 2 — every live non-uuid literal must be covered by the crosswalk.

    Zero rows (empty / fresh / already-migrated database) is a clean no-op, so
    ``alembic upgrade head`` still succeeds against an EMPTY database.
    """
    if not text_tables:
        return
    # pg_input_is_valid() is PostgreSQL >= 16. Name the requirement instead of
    # dying with an opaque UndefinedFunction on an older restore.
    version = int(conn.execute(sa.text("show server_version_num")).scalar_one())
    if version < 160000:
        raise RuntimeError(
            "migration 0011 requires PostgreSQL >= 16 for pg_input_is_valid(); this server "
            f"reports server_version_num={version}."
        )
    union = " UNION ".join(
        f"SELECT DISTINCT t.tenant_id FROM {t} t "
        f"WHERE t.tenant_id IS NOT NULL AND NOT pg_input_is_valid(t.tenant_id, 'uuid') "
        f"AND NOT {_cred('t.tenant_id')}"
        for t in text_tables
    )
    live = {r[0] for r in conn.execute(sa.text(union))}
    covered = {r[0] for r in conn.execute(sa.text(f"SELECT legacy_value FROM {CROSSWALK}"))}
    missing = sorted(live - covered)
    if missing:
        shown = ", ".join(repr(v) for v in missing[:20])
        raise RuntimeError(
            f"tenant_id remap is NOT total: {len(missing)} literal(s) live in this database "
            f"have no {CROSSWALK} row, so the ALTER would fail AFTER other tables were already "
            f"rewritten. First {min(len(missing), 20)}: {shown}. Add each to LEGACY_VALUES (or to "
            "REGISTRY if licences resolves it by exact name) and re-run. Do NOT widen the "
            "credential predicate to make one disappear."
        )


def _assert_columns(conn: sa.Connection, table: str, needed: tuple[str, ...]) -> None:
    have = _columns(conn, table)
    absent = [c for c in needed if c not in have]
    if absent:
        raise RuntimeError(
            f"{table} is missing column(s) {absent} that migration 0011 must group or sum over. "
            "The frozen key/additive list has drifted from the schema; summing the wrong columns "
            "would produce a wrong invoice, so this stops instead."
        )


def _remap_expr() -> str:
    """The post-remap tenant expression, as ONE definition.

    It used to be written out twice — once in ``_merge`` and once in
    ``_no_merge_guard`` — and when the ``::uuid`` cast was added it landed in
    only one of them. That is not cosmetic: without the cast the guard groups on
    TEXT, so two rows holding ``019DE842-…`` and ``019de842-…`` (the same uuid,
    two legal spellings) look like two distinct values, the guard reports zero
    duplicates, and the ALTER then dies on a unique violation with the whole
    migration — and the init container — rolling back and retrying forever.
    Reproduced 2026-08-25 before this was collapsed into one function.

    The cast is safe because ``_coverage_guard`` has already proved every
    remaining value is either crosswalked or uuid-parseable.
    """
    return (
        f"(CASE WHEN {_cred('t.tenant_id')} THEN '{NIL}' "
        "ELSE COALESCE(x.tenant_uuid::text, t.tenant_id) END)::uuid"
    )


def _merge(
    conn: sa.Connection, table: str, key: tuple[str, ...], additive: tuple[str, ...]
) -> None:
    """Step 4 — fold post-remap duplicates into the lowest ``id``.

    Computed against the CURRENT contents, never a frozen row list: ``unknown``
    and ``__unattributed__`` rows arrive daily, so a list captured at authoring
    time is stale before it deploys.

    ``(array_agg(id ORDER BY id))[1]`` rather than ``min(id)`` — the shape
    requirements_management' crosswalk helper uses — so this does not depend on
    a ``min(uuid)`` aggregate being present.

    GROUP BY is ``IS NOT DISTINCT FROM`` semantics; the NOT NULL condition in
    HAVING then restricts the merge to exactly the groups the LIVE (plain,
    NULLS-DISTINCT) unique constraint would reject. See the module docstring.

    The survivor's ``tenant_name`` is PINNED rather than inherited from the
    lowest ``id``; see the rule spelled out at ``name_agg`` below.
    """
    # An EMPTY table has no duplicates to fold, so the column assertion below
    # would only ever refuse a merge that has nothing to do. That is not
    # hypothetical: `billing_cost_daily.owner_provided` is created by the
    # runtime additive DDL in `src/db.py`, NOT by any migration, so on a
    # brand-new alembic-only database — CI's "upgrade head against an empty
    # postgres", and every fresh environment — the column does not exist yet
    # and `_assert_columns` aborted the WHOLE migration. Measured 2026-08-25,
    # and measured AGAIN after a mutation battery silently reverted this fix.
    # Skipping on empty keeps the guard's whole point: it still bites on any
    # database that holds rows, which is the only place summing the wrong
    # columns could produce a wrong invoice.
    if not conn.execute(sa.text(f"SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1)")).scalar():
        print(f"MERGE {table}: table is empty, nothing to fold", flush=True)
        return
    _assert_columns(conn, table, ("id", "tenant_id", *key, *additive))
    # ::uuid, not ::text — a value that is ALREADY a uuid takes the COALESCE
    # fallback and keeps whatever spelling the caller stored. Postgres accepts
    # uppercase, braced and dashless forms, all of which are DIFFERENT strings
    # for the same uuid, so grouping on text would leave such a pair unmerged
    # and it would die at the ALTER on a unique violation instead. The cast is
    # safe here because `_coverage_guard` has already proved every remaining
    # value is either crosswalked or uuid-parseable, and if that ever stopped
    # being true this raising is the correct outcome. (Measured 2026-08-25:
    # production holds 0 non-canonical spellings, so this is prevention.)
    remap = _remap_expr()
    key_sel = ", ".join(f"t.{c} AS {c}" for c in key)
    add_sel = ", ".join(f"t.{c} AS {c}" for c in additive)
    grp_cols = ", ".join(key)
    sums = ", ".join(f"sum({c}) AS sum_{c}" for c in additive)
    not_null = " AND ".join(f"{c} IS NOT NULL" for c in key)
    join_on = " AND ".join(f"r.{c} IS NOT DISTINCT FROM g.{c}" for c in key)
    sets = ", ".join(f"{c} = g.sum_{c}" for c in additive)
    # THE SURVIVOR-LABEL RULE (pin it in a test): the survivor is the lowest
    # `id`, but `id` is a UUIDv7, so inheriting its tenant_name would mean
    # "whichever row was inserted first". Where the table carries tenant_name,
    # the survivor instead takes the group's BEST available human label — any
    # value that is not NULL, not '', not 'unknown', not '__unattributed__' and
    # not credential-shaped ranks 0 and wins; everything else ranks 1. Ties
    # inside a rank break on the same ORDER BY id, so the outcome is
    # deterministic. If no row in the group has a good label the result is
    # exactly the old behaviour (lowest id), so this can only improve it.
    name_sel = name_agg = name_set = ""
    if "tenant_name" in _columns(conn, table):
        name_sel = ", t.tenant_name AS tenant_name"
        name_agg = (
            ", (array_agg(tenant_name ORDER BY CASE WHEN tenant_name IS NOT NULL "
            f"AND tenant_name NOT IN ('', 'unknown', '{UNATTRIBUTED}') "
            f"AND NOT {_cred_name('tenant_name')} THEN 0 ELSE 1 END, id))[1] AS best_tenant_name"
        )
        name_set = ", tenant_name = g.best_tenant_name"
    groups, deleted = conn.execute(
        sa.text(
            f"""
            WITH remapped AS (
                SELECT t.id AS id, {remap} AS new_tenant_id, {key_sel}, {add_sel}{name_sel}
                FROM {table} t
                LEFT JOIN {CROSSWALK} x
                       ON x.legacy_value = t.tenant_id AND x.source <> 'credential'
            ),
            grp AS (
                SELECT new_tenant_id, {grp_cols},
                       (array_agg(id ORDER BY id))[1] AS keep_id, {sums}{name_agg}
                FROM remapped
                GROUP BY new_tenant_id, {grp_cols}
                HAVING count(*) > 1 AND new_tenant_id IS NOT NULL AND {not_null}
            ),
            victims AS (
                SELECT r.id FROM remapped r JOIN grp g
                    ON r.new_tenant_id IS NOT DISTINCT FROM g.new_tenant_id AND {join_on}
                 WHERE r.id <> g.keep_id
            ),
            upd AS (
                UPDATE {table} k SET {sets}{name_set} FROM grp g
                 WHERE k.id = g.keep_id RETURNING 1
            ),
            del AS (
                DELETE FROM {table} d USING victims v WHERE d.id = v.id RETURNING 1
            )
            SELECT (SELECT count(*) FROM grp), (SELECT count(*) FROM del)
            """
        )
    ).one()
    print(f"MERGE {table}: groups={groups} rows_folded_and_deleted={deleted}", flush=True)


def _no_merge_guard(conn: sa.Connection, table: str, key: tuple[str, ...], where: str) -> None:
    """Step 4b — a post-remap duplicate on a config table RAISEs.

    There is no counter to sum here, so a merge would silently drop
    configuration. Measured 2026-08-25: 0 groups on all three. Zero rows is a
    clean no-op.
    """
    remap = _remap_expr()
    keys = ", ".join(key)
    dupes = conn.execute(
        sa.text(
            f"""
            SELECT count(*) FROM (
                SELECT 1 FROM (
                    SELECT {remap} AS new_tenant_id, t.*
                    FROM {table} t
                    LEFT JOIN {CROSSWALK} x
                           ON x.legacy_value = t.tenant_id AND x.source <> 'credential'
                    {where}
                ) r
                GROUP BY {keys}
                HAVING count(*) > 1
            ) q
            """
        )
    ).scalar_one()
    if dupes:
        raise RuntimeError(
            f"{table} would hold {dupes} duplicate group(s) on its unique key after the tenant_id "
            "remap, and it has no additive column — two configuration rows for one key cannot be "
            f"summed. Inspect {table} against {CROSSWALK}, decide by hand which row survives, "
            "delete the other, then re-run 0011."
        )


def _remap(conn: sa.Connection, text_tables: list[str]) -> None:
    """Step 5 — the actual UPDATE, from the crosswalk plus the credential sweep."""
    for table in text_tables:
        conn.execute(
            sa.text(
                f"UPDATE {table} t SET tenant_id = x.tenant_uuid::text "
                f"FROM {CROSSWALK} x "
                "WHERE t.tenant_id = x.legacy_value AND x.source <> 'credential'"
            )
        )
        # Credential rows are matched by SHAPE, never by value, and their
        # tenant_name is overwritten too: the token is duplicated into it
        # (measured, 4 rows in token_usage). Guarded on tenant_name actually
        # existing — only 9 of the 14 tables carry it.
        scrub = ""
        if "tenant_name" in _columns(conn, table):
            scrub = f", tenant_name = '{UNATTRIBUTED}'"
        conn.execute(
            sa.text(f"UPDATE {table} t SET tenant_id = '{NIL}'{scrub} WHERE {_cred('t.tenant_id')}")
        )


def upgrade() -> None:
    conn = op.get_bind()

    # 0. FIRST statement, before any catalog read or DDL: every lock this
    #    migration takes must be bounded, not just the ALTERs. The whole
    #    migration is one transaction (alembic/env.py:75-76), so a lock failure
    #    rolls everything back and costs a retry — whereas a statement left
    #    waiting on a live pod's lock blocks every reader queued behind it.
    op.execute("SET LOCAL lock_timeout = '10s'")

    # 1. Crosswalk, seeded before anything is touched.
    _create_crosswalk()
    _seed_crosswalk(conn)

    live = _tenant_columns(conn)
    text_tables = [t for t in TENANT_TABLES if live.get(t) not in (None, "uuid")]

    # 2. Ownership FIRST. Never die on a table this role does not own — and
    #    never let one raise a guard either: `work` is the only set of tables
    #    anything below is allowed to read, guard, rewrite, ALTER or index.
    owned = set(_owned(conn, list(live)))
    skipped = sorted(set(live) - owned)
    work = [t for t in text_tables if t in owned]

    _record_credentials(conn, work)

    # 3. Total coverage over exactly the tables that will be rewritten. Raises
    #    before any row is touched; an uncovered literal parked in a table this
    #    role cannot ALTER is not this migration's business.
    _coverage_guard(conn, work)

    # 4. Collisions the remap is about to create.
    for table in [t for t in MERGE_TABLES if t in work]:
        spec = MERGE_TABLES[table]
        _merge(conn, table, spec["key"], spec["additive"])
    for table in [t for t in NO_MERGE_KEYS if t in work]:
        _no_merge_guard(conn, table, NO_MERGE_KEYS[table], NO_MERGE_WHERE[table])

    # 5. Remap.
    _remap(conn, work)

    # 6. The two markup indexes cannot survive the cast: they index
    #    COALESCE(tenant_id, ''::character varying) and coalesce(uuid, '') has
    #    no type resolution. Dropped here, recreated at step 8.
    #    Gated on OWNING markup_rules, not merely on it existing: DROP INDEX on
    #    a table this role does not own fails with "must be owner of index" and
    #    would abort everything right after the guard printed SKIP not owner.
    if "markup_rules" in work:
        op.execute(f"DROP INDEX IF EXISTS {MARKUP_LOOKUP}")
        op.execute(f"DROP INDEX IF EXISTS {MARKUP_OPEN}")

    # 7. The type flip. lock_timeout (step 0) bounds the ACCESS EXCLUSIVE window.
    current = _tenant_columns(conn)
    for table in work:
        if current.get(table) == "uuid":  # re-run: nothing to do
            continue
        # billing_cost_events is the partitioned PARENT — this recurses into all
        # 13 children and rebuilds their 26 tenant indexes. Never alter a child.
        op.execute(f"ALTER TABLE {table} ALTER COLUMN tenant_id TYPE uuid USING tenant_id::uuid")

    # 8. Recreate the markup indexes with the cast, following the precedent the
    #    same index already sets for license_id (src/models/db/config.py:141).
    #    Gated on the SAME condition as the drop at step 6 — owning the table,
    #    which also implies it exists (a fresh alembic-only database may not
    #    have every ORM table yet). IF NOT EXISTS so a re-run is a no-op.
    if "markup_rules" in work:
        op.execute(
            f"CREATE INDEX IF NOT EXISTS {MARKUP_LOOKUP} ON markup_rules "
            "(COALESCE(tenant_id::text, ''), provider, model, effective_from DESC)"
        )
        op.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {MARKUP_OPEN} ON markup_rules "
            "(COALESCE(tenant_id::text, ''), COALESCE(license_id::text, ''), "
            "COALESCE(service_key, ''), COALESCE(provider, ''), COALESCE(model, '')) "
            "WHERE effective_until IS NULL"
        )

    # 9. Postcondition. Without this, "flipped" and "silently skipped" look the
    #    same in the log.
    final = _tenant_columns(conn)
    still_text = sorted(t for t, dt in final.items() if dt != "uuid" and t not in skipped)
    if still_text:
        raise RuntimeError(
            f"tenant_id is STILL not uuid on: {still_text}. Skipped by the ownership guard "
            f"(expected to remain varchar): {skipped or 'none'}. The migration reached its "
            "postcondition without flipping every table it owns — do not treat this run as "
            "applied."
        )
    if skipped:
        print(
            "tenant_id flip left "
            f"{len(skipped)} table(s) as varchar because this role cannot ALTER them: "
            f"{', '.join(skipped)}. Convert by hand (or transfer ownership); {CROSSWALK} holds "
            "the value mapping.",
            flush=True,
        )


def downgrade() -> None:
    """Reverse the column TYPE only.

    ``tenant_id_crosswalk`` and the merged rows deliberately STAY. A merge
    folded rows that were always one tenant and nothing records which counter
    came from which spelling, so re-splitting them is guesswork, not a
    reversal. Read ``tenant_id_crosswalk`` for the mapping.
    """
    conn = op.get_bind()

    # FIRST statement, same reasoning as upgrade() step 0: one transaction, so
    # a lock failure costs a retry while a waiting statement blocks readers.
    op.execute("SET LOCAL lock_timeout = '10s'")

    live = _tenant_columns(conn)
    owned = set(_owned(conn, list(live)))
    work = [t for t in TENANT_TABLES if live.get(t) == "uuid" and t in owned]

    # Ownership-gated, like upgrade() step 6 — and computed BEFORE the drop,
    # which is the whole point: DROP INDEX on a table this role does not own
    # fails with "must be owner of index" and aborts the transaction.
    if "markup_rules" in work:
        op.execute(f"DROP INDEX IF EXISTS {MARKUP_LOOKUP}")
        op.execute(f"DROP INDEX IF EXISTS {MARKUP_OPEN}")

    for table in work:
        op.execute(
            f"ALTER TABLE {table} ALTER COLUMN tenant_id TYPE VARCHAR(255) USING tenant_id::text"
        )

    # Back to the original coalesce form. These CANNOT be rendered from
    # Base.metadata here: the models ship as Uuid, so the helper would emit
    # `tenant_id::text` against a varchar column in some environments and the
    # original text form in others. Written out explicitly instead.
    if "markup_rules" in work:
        op.execute(
            f"CREATE INDEX IF NOT EXISTS {MARKUP_LOOKUP} ON markup_rules "
            "(COALESCE(tenant_id, ''::character varying), provider, model, effective_from DESC)"
        )
        op.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {MARKUP_OPEN} ON markup_rules "
            "(COALESCE(tenant_id, ''::character varying), COALESCE(license_id::text, ''), "
            "COALESCE(service_key, ''), COALESCE(provider, ''), COALESCE(model, '')) "
            "WHERE effective_until IS NULL"
        )
