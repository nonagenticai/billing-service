"""Canonical tenant-identifier resolution — the ONE place a tenant id is decided.

Since migration 0011 every ``tenant_id`` column is a real Postgres ``uuid``.
That changes the failure mode of a bad tenant identifier from "a junk row nobody
notices" into ``sqlalchemy.exc.DataError: invalid input syntax for type uuid``
raised at the first WHERE clause or INSERT that touches it. The defence has to
sit at the boundary — the HTTP header, the Redis-stream payload — not at the
SQL call site, because by the time SQL sees it the request is already lost.

Three rules this module exists to enforce:

1. **A name is resolved, never stored.** A name is looked up in the LIVE
   licences registry (see :func:`set_live_tenant_registry`) and, failing that,
   in ``LEGACY_TENANT_NAMES`` — a checked-in snapshot of the same registry, now
   demoted to a fallback rather than the only answer. The name is an *input alias*; the uuid is what lands in a column.
   Nothing here ever writes a name back. (It held SIX hand-picked names until
   2026-08-25, which meant a genuinely registered tenant such as ``marketing``
   or ``sales`` took a hard 400 from this service while ``n8n-dev`` worked —
   see the comment above the dict for the regeneration query.)
2. **No identifier is ever minted for an unknown input.** An unrecognised value
   resolves to ``None`` so the caller can reject it (HTTP 400) or attribute it
   to :data:`NIL_TENANT_ID`. The predecessor behaviour — falling back to the
   literals ``"unknown"``, ``"__admin__"`` or ``"__default__"`` — invented
   tenants that then accumulated real rows (13,670 of them under ``"unknown"``).
3. **Derivation is uuid5, not uuid7.** :func:`derived_tenant_id` hashes a legacy
   name into the RFC-4122 namespace below. A v7 uuid encodes a mint timestamp,
   and there is no honest mint time for a value that was already sitting in the
   table before anyone decided it needed a uuid — a v7 here would stamp "created
   at migration time" onto a tenant that is years old. v5 is a pure function of
   the name: deterministic, re-derivable, and it claims nothing about time.
"""

import asyncio
import logging
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from typing import Any

logger = logging.getLogger(__name__)

# RFC 4122 URL namespace. Byte-identical to the value the two predecessor
# migrations used to derive tenant uuids — it MUST NOT be changed or every
# previously derived id silently becomes a different tenant.
TENANT_NAMESPACE = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")

# The nil uuid, registered in ``licenses.tenants`` as ``default-anon``. This is
# already what emitters send when they have no tenant, so using it as the
# "unattributed" sink introduces no new identity — it names the one that exists.
NIL_TENANT_ID = "00000000-0000-0000-0000-000000000000"

# Replaces the ``"__default__"`` string sentinel in ``tenant_config.tenant_id``.
# Derived rather than hand-picked so the value is reproducible from the name
# alone by anyone reading a row in psql.
DEFAULT_CONFIG_TENANT_ID = str(uuid.uuid5(TENANT_NAMESPACE, "__default__"))

#: Legacy tenant NAME → its registry uuid. A name is RESOLVED here and never
#: stored; only the uuid reaches a column. This is a compatibility shim for
#: callers that still put a human label in ``X-Tenant-Id`` (notably ``fr``,
#: which forwards the raw Keycloak ``tenant_id`` claim, and ``license-authz``).
#:
#: It is a SNAPSHOT of the licences registry — the same source of truth the
#: 0011 migration uses for ``source='registry'`` — refreshed 2026-10-15.
#:
#: 2026-10-15 refresh: +35 names, all pure additions (no rename, no uuid
#: change), among them the per-app customer tenants that went live 2026-10-14
#: (``netcare``, ``netcare-cp``, ``svc-commonpractice``, ...). Live pods were
#: logging all 35 as ``missing_from_snapshot``; during a licences outage the
#: fallback would have parked their spend as ``unregistered_tenant``.
#:
#: ⛔ THE KEY COUNT IS NOT THE REGISTRY COUNT, AND THE DIFFERENCE IS THE POINT.
#: This map holds **116 keys = 111 current registry names + 5 RETIRED spellings**
#: (the retired ones are :data:`RENAMED_TENANT_ALIASES` below). The docstring
#: used to say "all 72 registered names" while the map held 75 keys, and the
#: mismatch was not a typo — it was drift nobody could see, in BOTH directions
#: at once. Measured against the live registry on 2026-09-05:
#:
#:   * SIX registered names were MISSING from this map, so every caller sending
#:     one took a hard 400 from this service: ``claude``,
#:     ``procedure-improvement``, ``prompt-improvement``, ``svc-kag``,
#:     ``svc-kag-eval-r46``, ``svc-w2v-shared``. Added.
#:   * TWO keys the registry no longer holds — ``platform-cp-4`` and
#:     ``platform-cp-5`` — were sitting here undeclared. They are the retired
#:     spellings of ``prompt-improvement`` (…0004) and
#:     ``procedure-improvement`` (…0005): same uuid, renamed name. KEPT, and now
#:     declared in :data:`RENAMED_TENANT_ALIASES` so the next regeneration
#:     cannot silently drop them.
#:
#: Do not hand-maintain that arithmetic again. ``tests/test_tenant_registry_shim.py``
#: asserts this map against ``tests/fixtures/licenses_tenants.txt`` — a
#: checked-in snapshot of the registry query below, refreshed by
#: ``scripts/refresh_tenant_registry_fixture.sh`` — in both directions, and
#: asserts the counts stated in this very docstring. A stale comment here is now
#: a red test, not a comment.
#:
#: This is the same defect recurring, not a new one. The map held SIX names,
#: chosen by hand, until 2026-08-25 — which meant a genuinely registered tenant
#: such as ``marketing``, ``sales``, ``platform-core`` or ``book-fulfillment``
#: got a hard 400 from this service while ``n8n-dev`` worked. That asymmetry was
#: the defect, not the shim. Widening the snapshot fixed the instance and left
#: the CLASS open: a hand-copied snapshot re-drifts the moment the registry
#: moves, and between 2026-08-25 and 2026-09-05 it drifted by eight entries. The
#: test is the actual fix; this comment is not.
#:
#: Regenerate with (on worker4, the prod postgres host):
#:   sudo -u postgres psql -d licenses -Atc "select name||'|'||id from tenants \
#:     where not name ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-' order by name"
#:
#: The exclusion is deliberate: one registry row has a NAME that is itself a
#: uuid string. ``canonical_tenant_id`` parses a uuid BEFORE consulting this
#: map, so such a name is unreachable here and listing it would only create a
#: second, contradictory meaning for that string.
#:
#: ``default-anon`` maps to the nil uuid because the registry says so. A caller
#: naming it lands on the unattributed bucket, which is what that tenant IS.
#:
#: An unknown name is still a 400. This shim resolves names that already exist;
#: it never mints one.
#:
#: ⛔ The query output is NOT the whole map. Names the registry has RENAMED are
#: kept here as aliases so the old spelling still resolves — see
#: :data:`RENAMED_TENANT_ALIASES` below and re-add every key it lists after any
#: regeneration, or you will 400 live callers that still send the old name.
LEGACY_TENANT_NAMES: dict[str, str] = {
    # licences: …0003 is renamed george-dev-portal-realm -> aa-agents. This
    # entry MUST land BEFORE the registry UPDATE runs, not after — see
    # RENAMED_TENANT_ALIASES below for why the order is not negotiable.
    "aa-agents": "0195f48b-d000-7000-8000-000000000003",
    "aa-tenant-proof": "019fa340-a5e4-709d-8047-f555f063068a",
    "ahtapol": "0195f48b-d000-7000-8000-000000000101",
    "better-results-group": "019fa2b2-1632-7c85-9403-8a7d9e74ab39",
    "book-fulfillment": "01a1109c-f895-7766-ba28-6ea56e56a5f7",
    "cdmsfirst": "01a1830f-377c-7730-9d86-6f9bc4894c36",
    "cdmsfirst-prod": "019f7b5f-64d3-71d5-81dc-0b9a53bf64c7",
    "claude": "01a03dd9-941a-7300-ad03-f099dcf93864",
    "claude-test": "01a13b8c-66b8-709e-81f4-2358fe957a15",
    "cp-e2e-2026-10-13": "01a135dd-698e-7000-864d-205ec171ad33",
    "cp-e2e-test-1014": "01a139e2-b2aa-7000-b40a-9f8074c69f87",
    "cp-e2e-test-20261014": "01a13b00-781f-7000-ae12-8a5c44864508",
    "default-anon": "00000000-0000-0000-0000-000000000000",
    "demo": "0195f48b-d000-7000-8000-000000000103",
    "e2e-smoke-qe95guoq": "019de842-c92b-7579-a7e6-5b79642d6c80",
    "e2e-smoke-xtzz3028": "019de842-c92b-75b0-9502-88a436832a67",
    "fr-test": "019efaa0-512b-733e-9e98-b1ac82a6f7ee",
    "fr-test-user": "0195f48b-d000-7000-8000-000000000099",
    "george-dekker": "90819343-37f8-4604-966b-d35368b8a167",
    "george-dev-portal-realm": "0195f48b-d000-7000-8000-000000000003",
    "internal-bench": "01a1117c-d9b0-72fc-aeda-a9d459931e41",
    "internal-dev-core": "01a1117c-d9b0-72fc-aeda-a9d3504ea2d5",
    "internal-dev-marketing": "01a1117c-d9b0-72fc-aeda-a9d216a14d53",
    "internal-probe": "01a115e9-7334-71bd-b264-3c4a396114de",
    "isolation-probe-test": "01a0fdb8-f785-7785-843d-0799f306cbbd",
    "marketing": "019edc61-9495-76bc-b65f-c92a8e6880df",
    "merqi": "639c333a-1613-4fbc-aa14-a7b1430dc0db",
    "n8n-dev": "019de842-c92a-7e37-8fdd-eb0d8c5396f9",
    "n8n-dev-portal-realm": "0195f48b-d000-7000-8000-000000000001",
    "n8n-test": "019de842-c92b-752e-b39c-30d64950e1cc",
    "netcare": "01a1ff14-338d-7293-93ea-28e5e17ac4b0",
    "netcare-cp": "01a13548-ab8a-7000-a2e0-497a779754a6",
    "netcare-legacy": "01a0ef72-31af-7561-936a-b8383b5fb6c8",
    "nonagentic": "019fe708-424a-7a31-bce8-da19020dc62b",
    "nonagentic-gtm": "e4f70a83-81ee-429a-abca-6999e3a0d24f",
    "ola-armatys": "019edc48-6060-76e9-8d8f-2e720e12c609",
    "platform-core": "0195f48b-d000-7000-8000-000000000002",
    # ALIASES, not registry names — see RENAMED_TENANT_ALIASES below. licences
    # #170 renamed these two rows; both spellings must keep resolving.
    "platform-cp-1": "0195f48b-d000-7000-8000-000000000001",
    "platform-cp-3": "0195f48b-d000-7000-8000-000000000003",
    # Retired spellings of prompt-improvement / procedure-improvement.
    # The registry renamed these two rows; both were sitting here
    # UNDECLARED until 2026-09-05, i.e. one regeneration away from being
    # silently dropped. Now in RENAMED_TENANT_ALIASES.
    "platform-cp-4": "0195f48b-d000-7000-8000-000000000004",
    "platform-cp-5": "0195f48b-d000-7000-8000-000000000005",
    "portal": "019de842-c92b-7564-b932-98240d274b3b",
    "portal-render-e2e": "01a1113b-674f-72b0-a84b-dc746d39afca",
    "procedure-improvement": "0195f48b-d000-7000-8000-000000000005",
    "prompt-improvement": "0195f48b-d000-7000-8000-000000000004",
    "raisemytickets": "01a0ae64-3703-76a6-8e05-9a4826428d32",
    "rmt-e2e": "019fb1f4-7342-7bb7-8ac6-2ff2fcda645f",
    "sales": "019edc61-9495-7ee7-93dd-ea9e1f687f42",
    "subhankar-kag": "019edc1e-313d-779d-9330-fe013c705be8",
    "subhankar-kag-2": "918c1948-0f9e-4f1d-b15e-7ca1a950a184",
    "svc-cdmsfirst-n8n": "97d33e0a-fc56-5ad6-8c32-1ccc565ff327",
    "svc-coding-ai": "e32db873-6197-5cbe-8d6f-f800ff015b37",
    "svc-coding-analysis": "3533c51e-19e6-5a29-a636-7269f941a256",
    "svc-coding-containers": "71189c26-d5c2-50ef-94b6-719db4af5f9f",
    "svc-coding-gateway": "e7b1d902-5bae-5bd8-9fe2-1c6ff29b7b63",
    "svc-coding-github": "83ecc1ad-54bd-5de7-9b56-0f1707dcba8e",
    "svc-coding-realtime": "41020e1b-8ff6-54dd-bb36-725da5046e5c",
    "svc-coding-repo-jobs": "91c77f12-c7fa-5ab1-bfd8-25f8e1874315",
    "svc-coding-security": "6aeb0926-6ffc-59ce-b668-59eeb318de78",
    "svc-coding-testing": "526378a8-0b20-5784-b5ce-d0fdc7a42d2b",
    "svc-commonpractice": "b8e94a81-dbd3-5f75-8a58-45deceaef000",
    "svc-control-relay": "9b820614-8b03-5d1c-856b-d4c51a5b145c",
    "svc-control-service-w2v-partner": "c44cf5dd-c721-57e3-acb2-64305acb3dc0",
    "svc-core-app-mlx": "631a1fc3-41a1-5ebd-8515-4b8617cb585f",
    "svc-discovery-engine": "f5e6212e-5a16-5a16-9864-0ce1ab1a08ee",
    "svc-evidence-review": "b1122557-09db-534b-8a38-0dde459a7564",
    "svc-fr-backend": "d4804d48-2761-50d7-9302-fb366fbcad1c",
    "svc-fr-backend-kag": "dfb9ca84-05f9-510b-93ae-b8dda21ebd4a",
    "svc-fr-backend-pm": "db988f11-b36f-53ad-83a3-ebefb0657d83",
    "svc-growth": "365ef796-56c9-5b32-b28a-c05489ed59c9",
    "svc-hirag": "9ec0a01d-78cd-54e1-9f40-8e48e93f6707",
    "svc-hirag-graph": "c361d84f-8a42-5dfa-aaa9-0339658f22ae",
    "svc-image-e2e-customer": "dd4eb320-59d8-5359-bb4a-5adb8bb77a01",
    "svc-image-gateway": "d6cffb03-db1b-58c6-94ba-e8a1d9f3c507",
    "svc-inference-router": "b56c7aae-2fbc-56fe-bab2-6240e320c8ac",
    "svc-ingestion": "0e2bc289-1cd5-5d82-a100-e4073f501058",
    "svc-ingestion-graph": "ef9735e5-0c71-56e0-852b-3ca601177bb6",
    "svc-kag": "291c4cf5-fc88-56d5-9254-ac02869728a8",
    "svc-kag-eval-r46": "4dd78137-4f5f-531d-b58b-385bded3914c",
    "svc-kag-frontend-pm": "f8c3ee18-b02c-5a6f-a5af-f79aaf1fce23",
    "svc-kag-graph": "a8035a90-4210-5900-9529-45b3b90b72a2",
    "svc-kag-graph-reconciler": "17c61bc0-027b-5964-8d0f-e0e9d32c3cd0",
    "svc-kag-karma": "f172bf91-2dd5-5386-ae73-704f31f4f0a9",
    "svc-kag-orchestrator-graph": "15fc5490-690b-5b60-9449-d42b9548386c",
    "svc-karma": "e929ef57-5670-5c26-85ac-eab725866516",
    "svc-marketing": "63e46a0c-e2b0-5ae3-a04f-c9888280827e",
    "svc-marketing-t-c46c9378": "c46c9378-eef5-57d6-b5f9-307e86db915d",
    "svc-media-discovery": "bbdb4939-4c4c-5b4c-8286-3ffd3921b8a7",
    "svc-modality-gateway": "0a04dd67-40ae-5c02-a087-0a6227cfb3f2",
    "svc-modality-gpuproof-e2e": "a2ce50eb-f0d7-54e5-8ffe-975aece729c5",
    "svc-modality-gw-e2e": "65a6416c-a372-5a00-b50e-6ab5193f4a6d",
    "svc-modality-phaseb-e2e": "74c2fd20-da0c-57ba-9020-dd4dba736a63",
    "svc-n8n-aa": "92e8d32c-850a-52ff-997e-b6f5fcb94231",
    "svc-n8n-local-conveyor": "5f868d42-03f8-51f0-a93c-3e83070dc196",
    "svc-nlp-graph": "4eeb06ef-1312-5bc2-8a60-0945d30519cb",
    "svc-nlp-service": "c13dfe28-f5af-5656-8786-51f34b2629ec",
    "svc-platform-console": "a127721c-3b2f-5c4a-b77f-8a413c2c055f",
    "svc-probe-gwflip-20260813": "d535bf4c-062b-5108-b992-a685b6ad59a4",
    "svc-probe-tenant-header-20260812": "eba3af2b-6d9b-5e24-8294-6eddca146d8f",
    "svc-probe-tenant-header-20260812b": "0fbaaead-1752-54e2-843a-e4bbd946bb48",
    "svc-probe-vouch-20260813": "593bc1cc-eb77-55b7-b8a7-20c469077609",
    "svc-requirements": "01a0138c-a6bc-75e7-9263-e3894f2eb94c",
    "svc-requirements-pm": "55d23f87-49c9-5ddb-b34b-b3c9c75d27ad",
    "svc-sales": "506b6958-4c12-5fda-9822-a67508e4bb08",
    "svc-scientific-processor": "16f5fb5b-141d-5fc1-b513-24ca7d4a31b4",
    "svc-scientific-processor-graph": "c9669d15-b85a-567f-b2dd-534ddad0c7c1",
    "svc-tts-synth": "333c001e-03eb-5508-901c-b05409b0df85",
    "svc-vlm-smoke": "a3c1e697-6c05-5717-9612-f7fc8743d92d",
    "svc-w2v-cell-m4-gate-tenant": "4cc78f13-456c-5243-a026-5c4c1b67aa77",
    "svc-w2v-cell-m4-orchestrator": "396f3448-ccc1-584a-92f8-a1a3de3fbe1d",
    "svc-w2v-cell-m4-partner": "baad6bac-7541-5d9f-b209-423b94bcdc5f",
    "svc-w2v-shared": "4af92cd8-4d13-54f1-aaf9-94ee2caaf823",
    "system": "00000000-0000-0000-0000-000000000001",
    "tenant-dev": "3806d6e5-4698-571e-9bb2-22ffd076ac73",
    "w2v-partner-provisioning": "01a11fae-b5df-72be-9c58-41bc7d791f17",
    "zz-s62-probe-a1": "019fef97-bc1a-74b1-89e6-5d0574210203",
}

#: RETIRED SPELLING → the name the registry holds for that SAME uuid today.
#:
#: Every key here is ALSO a key of :data:`LEGACY_TENANT_NAMES`, mapping to the
#: identical uuid as its value-name does. That duplication is the entire point:
#: a rename in the licences registry must never become a 400 for a caller that
#: still sends the old spelling. Resolution is by name, so the two spellings are
#: two doors onto one tenant — nothing downstream can tell which was used,
#: because only the uuid is ever stored.
#:
#: licences #170 (merged as 6e195da, migration 024) renamed two rows that had
#: been registered in 2026-07-10's migration 014 under invented placeholder
#: labels. Each is a real tenant's canonical uuid7 in the Keycloak PORTAL realm
#: — the literal ``tenant_id`` claim inside live user JWTs, which ``fr``
#: forwards verbatim into ``X-Tenant-Id``. The placeholder spelling is therefore
#: in flight RIGHT NOW and cannot be dropped on the registry's schedule.
#:
#: ⛔ REGENERATING :data:`LEGACY_TENANT_NAMES` FROM THE REGISTRY DROPS THESE.
#: The query in the comment above returns only names the registry currently
#: holds, and after migration 024 that no longer includes ``platform-cp-1`` or
#: ``platform-cp-3``. Paste the query output in and the old spellings vanish,
#: silently, and every caller still sending one starts taking a hard 400. So a
#: regeneration is: paste the query result, THEN re-add every key below.
#: ``tests/test_auth_deps.py::TestRenamedTenantNamesKeepResolving`` fails if a
#: regeneration forgets — that test is the actual guard, this comment is not.
#:
#: An entry may only be REMOVED once the old spelling is measured to be absent
#: from live traffic — the same bar licences #170 held itself to before renaming
#: (0 by-name lookups over 17h). Age is not evidence.
RENAMED_TENANT_ALIASES: dict[str, str] = {
    # licences #170: …0001 is n8n-dev in the portal realm, …0003 is george-dev.
    "platform-cp-1": "n8n-dev-portal-realm",
    "platform-cp-3": "george-dev-portal-realm",
    # …0003 again: the registry renames george-dev-portal-realm -> aa-agents.
    # This alias SHIPS FIRST and the registry UPDATE runs only once this pod is
    # live. Reversed, every caller still sending the old spelling — which is the
    # literal Keycloak ``tenant_id`` claim ``fr`` forwards — takes a hard 400
    # between the UPDATE and the deploy. Note ``platform-cp-3`` above now aliases
    # a name that is itself retired; both still resolve, because resolution goes
    # through LEGACY_TENANT_NAMES, which holds all three spellings of …0003.
    "george-dev-portal-realm": "aa-agents",
    # Found UNDECLARED 2026-09-05. Both keys were already in
    # LEGACY_TENANT_NAMES and both uuids are still live in the registry under a
    # NEW name, so resolution was correct — but nothing recorded that they were
    # retired spellings, which meant the very next regeneration would have
    # deleted them without a word. That is the failure mode this dict exists to
    # prevent, and it was one paste away from happening.
    "platform-cp-4": "prompt-improvement",
    "platform-cp-5": "procedure-improvement",
}


#: FABRICATED TENANTS: they resolve, they are never registered, they never bill.
#:
#: ``"system"`` is not a customer. It is a placeholder an emitter sends when it
#: has no tenant to name, and because it is a KEY of
#: :data:`LEGACY_TENANT_NAMES` it resolved to …0001 and that uuid was, in turn,
#: a VALUE of the same dict — so :data:`_REGISTERED_TENANT_IDS` vouched for it
#: and every such event billed as a registered tenant. Measured on the prod
#: billing primary 2026-09-16: 1,207 rows / $23.5395, every one of them
#: ``service_key='inference-router'``, all billing under a tenant nobody owns.  # pragma: allowlist secret
#:
#: ⛔ THE FIX IS NOT TO DELETE THE NAME. Dropping ``"system"`` from
#: :data:`LEGACY_TENANT_NAMES` makes :func:`resolve_tenant_name_to_id` answer
#: ``None``, which makes ``validate_cost_event`` return ``"invalid_tenant_id"``,
#: which DEAD-LETTERS the event to Redis ``billing:costs:dead`` — trimmed at
#: 720h, capped at 100k, payload redacted on the way in. No Postgres row is
#: written at all, so the money record is destroyed rather than parked, and the
#: unregistered/park path in ``services.cost_flusher`` is never reached: it sits
#: 127 lines AFTER the ``continue`` that quarantines the event. That is the
#: strictly-worse direction :func:`canonical_tenant_id` already warns about.
#:
#: So the name keeps resolving and the ID stops being vouched for. The event
#: survives validation, reaches the flusher, fails
#: :func:`is_registered_tenant_id`, and is PARKED in ``billing_cost_events``
#: with ``excluded_from_billing = true`` and
#: ``exclusion_reason = 'unregistered_tenant'`` — visible in SQL, reversible by
#: one UPDATE, and counted on
#: ``billing_cost_events_unregistered_tenant_total{action="excluded"}``.
#:
#: ⚠️ This is what makes the armed gate do anything. ``exclusion_reason =
#: 'unregistered_tenant'`` has parked ZERO rows all-time despite
#: ``BILLING_EXCLUDE_UNREGISTERED_TENANTS=true`` in production, because until
#: now every id reaching the check was vouched for by the snapshot.
SENTINEL_TENANT_IDS: frozenset[str] = frozenset(
    {
        "00000000-0000-0000-0000-000000000001",  # LEGACY_TENANT_NAMES["system"]
    }
)

#: Known test/synthetic fixture values found sitting in real tables during the
#: R15 estate audit (measured 2026-09-16: 6 of 37 distinct divergent tenant
#: ids). None of these was ever registered and none is a real customer — they
#: are transparently fabricated (repeating digits, the word ``dead``/``beef``/
#: ``cd15f57`` spelled in hex, an all-zero prefix). Checked EXACTLY, and checked
#: BEFORE the uuid-version classification below: several of these parse as a
#: well-formed v4 or v7 uuid by pure bit-pattern accident
#: (``0195f48b-dead-7000-8000-00000000beef`` is a syntactic v7,
#: ``0000abcd-0000-4000-8000-0000000000aa`` and
#: ``00000000-0000-4000-8000-00000000c057`` are syntactic v4s), and this set
#: must win that tie so the classifier names them for what they are —
#: leftover fixture data — rather than filing them beside genuine v4/v7
#: production traffic.
SYNTHETIC_TENANT_IDS: frozenset[str] = frozenset(
    {
        "11111111-1111-1111-1111-111111111111",
        "0195f48b-dead-7000-8000-00000000beef",  # pragma: allowlist secret
        "0000abcd-0000-4000-8000-0000000000aa",  # pragma: allowlist secret
        "00000000-0000-0000-0000-00000cd15f57",  # pragma: allowlist secret
        "00000000-0000-4000-8000-00000000c057",  # pragma: allowlist secret
        "0199aaaa-0000-7000-8000-0000000000dd",  # pragma: allowlist secret
    }
)

#: Classification vocabulary for :func:`classify_tenant_id`, in precedence
#: order. ``"registered"`` is checked first (any registered id is reported as
#: that and nothing else, whatever its bit pattern happens to look like); the
#: remaining five are the population :func:`classify_tenant_id` can hand back
#: for an id NO registry vouches for. Read together they answer "of everything
#: this service resolved, how much was real" without needing a second lookup —
#: see ``billing_tenant_id_classification_total`` in ``observability.metrics``.
TENANT_ID_CLASSIFICATIONS: tuple[str, ...] = (
    "registered",
    "synthetic",
    "v5_derived",
    "v7",
    "v4",
    "other",
)

#: The subset of :data:`TENANT_ID_CLASSIFICATIONS` that means "not registered" —
#: i.e. the population a write-path REJECT (see
#: :data:`settings.Settings.billing_tenant_id_classification_enforce`) would
#: refuse. Derived as a tuple rather than "classification != 'registered'" at
#: each call site for the same reason :data:`_UNVOUCHED_SOURCES` is a named set
#: rather than an inline comparison: a new classification cannot silently
#: default to "safe to reject" just because nobody updated a boolean expression.
TENANT_ID_DIVERGENCE_CLASSES: tuple[str, ...] = TENANT_ID_CLASSIFICATIONS[1:]


def classify_tenant_id(canon: str) -> str:
    """Classify a canonical uuid into one member of :data:`TENANT_ID_CLASSIFICATIONS`.

    Pure: no counting, no I/O, no refresh — same discipline as
    :func:`registration_source`, which this delegates to for the "is it
    registered at all" question. Precedence, and why each rung exists:

    1. ``"registered"`` — :func:`is_registered_tenant_id` vouches for it. This
       is checked FIRST and unconditionally wins: a registered id must never be
       reported as a version bucket just because its bit pattern happens to
       look like one — most real tenant ids ARE v4 or v7 by construction, and
       reporting them there would drown the five-row synthetic/derived
       population this classifier exists to separate out.
    2. ``"synthetic"`` — an EXACT member of :data:`SYNTHETIC_TENANT_IDS`, known
       test/fixture values that leaked into real tables. Checked before the
       version rungs for the reason documented on that set: several of them
       parse as a syntactic v4 or v7 by accident.
    3. ``"v5_derived"`` — RFC 4122 version 5 (a name-hash, the same shape
       :func:`derived_tenant_id` produces for a *registered* legacy name). An
       unregistered v5 is therefore not a random id — it was derived from SOME
       name, just not one any registry entry currently owns.
    4. ``"v7"`` — RFC 4122 version 7 (time-ordered, mint-timestamp-bearing).
       An unregistered v7 looks like something minted by real code at a real
       moment, which is a different remediation story from a hand-typed test
       fixture.
    5. ``"v4"`` — RFC 4122 version 4 (random). The ordinary shape a caller's
       own uuid4() produces.
    6. ``"other"`` — anything else: a different RFC 4122 version (v1/v2/v3/v6,
       none of which this estate's divergent population contained as of the
       2026-09-16 measurement), or a value that superficially parsed as a uuid
       upstream but whose version field this classifier cannot read. Never
       silently folded into v4 — an unrecognised shape is a fact worth keeping
       visible, not noise to average away.

    Raises nothing: :func:`uuid.UUID` failing on an already-canonicalised
    string would be this module's own bug, but the version lookup is still
    guarded so a classification call can never become the reason a request
    fails where resolution itself did not.
    """
    if is_registered_tenant_id(canon):
        return "registered"
    if canon in SYNTHETIC_TENANT_IDS:
        return "synthetic"
    try:
        version = uuid.UUID(canon).version
    except ValueError, AttributeError, TypeError:
        return "other"
    if version == 5:
        return "v5_derived"
    if version == 7:
        return "v7"
    if version == 4:
        return "v4"
    return "other"


def _count_tenant_id_classification(classification: str) -> None:
    """Count one :func:`classify_tenant_id` verdict. Never raises.

    Function-local import for the same circularity reason as
    :func:`_note_unregistered_tenant` — ``observability.metrics`` imports
    ``services._cost_schema``, which imports this module.
    """
    try:
        from observability.metrics import tenant_id_classification_total

        tenant_id_classification_total.labels(classification=classification).inc()
    except Exception:  # pragma: no cover - observability must not break auth
        pass


def _count_tenant_id_classification_rejected(classification: str) -> None:
    """Count one write-path REJECT issued by the enforcing mode. Never raises."""
    try:
        from observability.metrics import tenant_id_classification_rejected_total

        tenant_id_classification_rejected_total.labels(classification=classification).inc()
    except Exception:  # pragma: no cover - observability must not break auth
        pass


def _tenant_id_classification_enforced() -> bool:
    """Is the write-path guard armed to REJECT rather than only observe?

    Reads ``settings.Settings.billing_tenant_id_classification_enforce``,
    default ``False``. Any failure to read settings (not yet configured, a
    circular-import edge during startup, a stubbed settings object in a test
    that does not define the field) is treated as **disarmed** — the same
    fail-open discipline as the rest of this module: an observability/config
    defect must never turn a resolvable tenant into a 400, and it certainly
    must not turn one into a rejection nobody armed.
    """
    try:
        from settings import get_settings

        return bool(get_settings().billing_tenant_id_classification_enforce)
    except Exception:  # pragma: no cover - config must not break resolution
        return False


#: Every uuid the registry snapshot in :data:`LEGACY_TENANT_NAMES` vouches for,
#: plus the one id this service derives for itself. A value that PARSES as a
#: uuid but is absent from this set was accepted by step (a) of
#: :func:`canonical_tenant_id` with no registry check whatsoever.
#:
#: :data:`SENTINEL_TENANT_IDS` is SUBTRACTED, not merely absent: the sentinel is
#: a value of :data:`LEGACY_TENANT_NAMES`, so the union above puts it in unless
#: it is taken back out. Subtracting here keeps the set honest on its own terms;
#: :func:`registration_source` is what makes the verdict robust against the LIVE
#: overlay re-vouching for it.
_REGISTERED_TENANT_IDS: frozenset[str] = (
    frozenset(LEGACY_TENANT_NAMES.values())
    | {
        DEFAULT_CONFIG_TENANT_ID,
    }
) - SENTINEL_TENANT_IDS

#: Ids already WARNed about, so the log names each offender once per process
#: instead of once per request. The COUNTER carries the full volume; this set
#: only bounds the log. Capped so a caller spraying random uuids cannot grow it
#: without limit — past the cap the log goes quiet, the counter does not.
_warned_unregistered: set[str] = set()
_WARNED_UNREGISTERED_CAP = 256


def _note_unregistered_tenant(canon: str) -> None:
    """Count and (once per id) log a uuid no registry entry vouches for.

    FAIL OPEN, on purpose. This does NOT reject and MUST NOT start to: rejecting
    before the denominator is measured silently drops live spend. Two ids from
    ``worker_management``'s ``principle_consistency`` were already metering
    ~$30.03/30d through this path when the counter was added, and neither is in
    the licences registry. Close the gate only once this series shows what
    closing it would actually cost.

    Every failure here is swallowed: an observability defect must never turn a
    resolvable tenant into a 400. The import is function-local because
    ``observability.metrics`` imports ``services._cost_schema``, which imports
    this module — a module-level import is a circular import.
    """
    try:
        from observability.metrics import tenant_unregistered_total

        tenant_unregistered_total.inc()
    except Exception:  # pragma: no cover - observability must not break auth
        pass
    try:
        if canon not in _warned_unregistered:
            if len(_warned_unregistered) < _WARNED_UNREGISTERED_CAP:
                _warned_unregistered.add(canon)
            logger.warning(
                "tenant_id %s is a well-formed uuid that no registry entry "
                "vouches for; accepted (fail-open) and counted on "
                "billing_tenant_unregistered_total",
                canon,
            )
    except Exception:  # pragma: no cover
        pass


# ---------------------------------------------------------------------------
# The LIVE licences registry, overlaid on the snapshot above
# ---------------------------------------------------------------------------
#
# ``LEGACY_TENANT_NAMES`` is a hand-regenerated snapshot, and its own comments
# record the failure mode twice: it held six hand-picked names until 2026-08-25,
# and between then and 2026-09-05 it drifted by eight entries in BOTH directions
# at once while looking perfectly healthy. ``tests/test_tenant_registry_shim.py``
# catches that in CI against a checked-in fixture; nothing caught it in
# production, where a registered tenant absent from the map takes a hard 400.
#
# So the registry is now read LIVE — ``GET {licenses_url}/internal/tenants``,
# through the client ``services.tenant_names`` already owns, refreshed on a loop
# in ``main.lifespan`` — and installed here as an OVERLAY consulted first. The
# call was verified from a billing pod on 2026-09-05: HTTP 200, 59 tenants. (The
# "billing gets 401/403 on that endpoint in prod" note that stood in this repo
# since 2026-08-28 is STALE; do not reason from it.)
#
# THREE PROPERTIES THIS MUST KEEP, all of them load-bearing:
#
# 1. FAIL SOFT, ALWAYS. Every failure mode — licences down, 401, timeout, a
#    shape change, no refresh ever having landed — degrades to the snapshot,
#    which is exactly today's behaviour. A registry outage must never become a
#    400 storm: this module sits on the hot path for every inbound header AND
#    every Redis-stream payload, so a hard dependency here would take metering
#    down with licences. The overlay is therefore never expired on age either —
#    a stale live map still beats the snapshot, and expiring it would convert an
#    outage into a resolution CHANGE at the worst possible moment.
# 2. NEVER MINT. The overlay can only ADD names and can only be built from ids
#    the registry itself returned; a value that does not parse as a uuid is
#    dropped, and an unknown name still resolves to ``None`` (rule 2 of the
#    module docstring). The predecessor's invented tenants accumulated 13,670
#    real rows.
# 3. NEVER SHRINK. A name the snapshot holds and the live registry does not
#    keeps resolving from the snapshot — that set is precisely
#    ``RENAMED_TENANT_ALIASES``, the retired spellings live callers still send.
#    "The registry no longer lists it" is not evidence that nobody sends it.
#
#: The live overlay: registry NAME -> registry uuid. Empty until the first
#: successful refresh, which is the ONLY state in which behaviour is identical
#: to what shipped before this existed.
_live_tenant_names: dict[str, str] = {}

#: The uuids the overlay vouches for. Read by :func:`is_registered_tenant_id`,
#: which UNIONS it with the snapshot's ids — never replaces them. A live refresh
#: can only make the "unregistered tenant" accusation rarer, never more common,
#: so a licences blip cannot cause rows to be parked as unregistered.
_live_tenant_ids: frozenset[str] = frozenset()

#: The REVERSE view of the overlay: registry uuid -> registry NAME. Derived from
#: the same install, never fetched separately, so the two directions can never
#: disagree about what the registry said.
#:
#: It exists because the WRITE path needs it. ``services.cost_flusher`` stores a
#: display label in ``billing_cost_events.tenant_name`` at the moment the row is
#: minted, and since migration 0011 the emitters send a uuid as ``tenant_id`` and
#: no ``tenant_name`` at all — so ``original_tenant_label`` echoed that uuid into
#: the display column on 66,103 of 66,103 live rows. Read-time resolution
#: (``/dashboard/costs``, the CSV export) papered over it on exactly two
#: surfaces; every SQL consumer, every rollup and ``billing_cost_daily``
#: .tenant_name still saw noise.
#:
#: A uuid claimed by TWO names is dropped, exactly as an ambiguous NAME is
#: dropped in the forward direction: picking one would print somebody else's
#: label next to this tenant's spend, and falling back to the uuid is a wrong
#: answer that is at least visibly wrong (and counted — see
#: ``billing_placeholder_value_total{field="tenant_name"}``).
_live_tenant_labels: dict[str, str] = {}

#: Names the live registry lists on MORE THAN ONE tenant uuid.
#:
#: Since the licences tenant-ownership change (tenant names unique per OWNING
#: APP, not globally) two apps may each own a tenant called ``netcare``. A bare
#: name then does not identify a tenant, and :func:`resolve_tenant_name_to_id`
#: answers ``None`` for every name in this set — it does NOT fall through to the
#: snapshot either, because the snapshot's answer would be a pick too, just an
#: older one. Counted on ``billing_tenant_name_ambiguous_total{name}``.
#:
#: Both uuids are still in :data:`_live_tenant_ids` and :data:`_live_tenant_labels`:
#: each is a registered tenant with an unambiguous label. Only the NAME -> id
#: direction is unanswerable.
_live_ambiguous_names: frozenset[str] = frozenset()

#: ``time.monotonic()`` of the last successful install, or None. Monotonic
#: because this only ever feeds an AGE, and a wall-clock step would make the
#: age lie.
_live_refreshed_at: float | None = None


def _count_name_resolution(source: str) -> None:
    """Count one NAME resolution by what answered: live / snapshot / unknown.

    The import is function-local for the same reason
    :func:`_note_unregistered_tenant`'s is: ``observability.metrics`` imports
    ``services._cost_schema``, which imports this module.
    """
    try:
        from observability.metrics import tenant_name_resolution_total

        tenant_name_resolution_total.labels(source=source).inc()
    except Exception:  # pragma: no cover - observability must not break auth
        pass


# ---------------------------------------------------------------------------
# Freshness, and the demand refresh that makes the hot path a LIVE consult
# ---------------------------------------------------------------------------
#
# ``docs/UNREGISTERED_TENANT_GATE.md`` states two preconditions for arming
# ``BILLING_EXCLUDE_UNREGISTERED_TENANTS``. The second one is:
#
#   "The snapshot-staleness root cause above is fixed — billing can consult the
#    registry live, or the refresh is automated and verified. Arming against a
#    stale snapshot re-creates the exact defect this document exists to
#    prevent."
#
# The background loop (``services.tenant_names.run_tenant_registry_refresh_loop``)
# is the "automated" half and it already runs on every replica. What was missing
# is the part a reader can PROVE and the part that closes the blind window:
#
#   * proof — nothing said whether an id was vouched for by the LIVE registry or
#     by the checked-in snapshot, so "the overlay is wired up" and "the overlay
#     has been empty since boot and the snapshot is answering everything" read
#     identically. That is :data:`tenant_registration_source_total`.
#   * the blind window — between two loop ticks, a tenant registered 30 seconds
#     ago is unknown to both maps. The gate, once armed, would park its spend.
#     :func:`_request_registry_refresh` turns that window from one refresh
#     interval into one cooldown, by asking for a refresh OFF the request path
#     the moment the hot path meets an id nothing vouches for.
#
# ⛔ NEITHER OF THESE MAY CHANGE A VERDICT. The refresh is scheduled, never
# awaited; the answer returned to this caller is the answer the overlay already
# held. A licences blip therefore still degrades to the snapshot, and the only
# observable difference is a counter label. Fail SAFE, not closed.

#: An overlay older than this is not evidence of anything. 3x the default
#: ``tenant_registry_refresh_seconds`` (300s), which is the SAME multiple and
#: the same reasoning ``settings.budget_write_requires_registered_tenant``
#: already documents for :func:`write_gate_refuses_tenant`: one missed tick is
#: still "fresh", a sustained outage is not. Kept as a module constant rather
#: than read from settings so this module stays importable by pydantic
#: validators with no settings dependency.
REGISTRY_STALE_AFTER_SECONDS: float = 900.0

#: Minimum seconds between two demand refreshes. Matches the 30s floor
#: ``run_tenant_registry_refresh_loop`` already puts under its interval, so the
#: worst case a hostile caller can produce — a fresh random uuid per event, every
#: event unknown — is two licences requests a minute, not one per cost event.
DEMAND_REFRESH_COOLDOWN_SECONDS: float = 30.0

#: Zero-arg callable returning an awaitable that refreshes the overlay, or None.
#: Installed by ``services.tenant_names.install_demand_refresh_hook`` from
#: ``main.lifespan``. A hook is how this module asks for I/O without importing
#: httpx, settings or the token provider — the same split
#: :func:`set_live_tenant_registry` already makes between "fetch" and "install".
_refresh_hook: Callable[[], Any] | None = None

#: ``time.monotonic()`` of the last demand refresh REQUEST (not completion).
_last_demand_refresh: float | None = None


def set_registry_refresh_hook(hook: Callable[[], Any] | None) -> None:
    """Install (or clear, with ``None``) the demand-refresh hook. Never raises."""
    global _refresh_hook, _last_demand_refresh
    _refresh_hook = hook
    _last_demand_refresh = None


def live_registry_is_fresh() -> bool:
    """True when a successful refresh landed within the staleness window.

    False means ABSENCE FROM THE OVERLAY IS NOT EVIDENCE — either no refresh has
    ever landed (age ``-1.0``) or the last one is older than
    :data:`REGISTRY_STALE_AFTER_SECONDS`. Callers that would REFUSE something
    must read this before treating "not in the registry" as a fact.
    """
    _, age = live_tenant_registry_stats()
    return 0.0 <= age <= REGISTRY_STALE_AFTER_SECONDS


def _request_registry_refresh() -> None:
    """Ask for an off-request-path refresh of the overlay. Never raises, never blocks.

    Rate-limited by :data:`DEMAND_REFRESH_COOLDOWN_SECONDS` and scheduled onto
    the running event loop with ``create_task`` — the caller does not await it
    and does not see its result. Three ways this quietly does nothing, all of
    them correct:

    * no hook installed (a test, a script, a management command),
    * no running event loop (this module is called from sync code, including
      pydantic validators),
    * the cooldown has not elapsed.

    The task reference is deliberately not retained: the refresh is best-effort
    and ``refresh_tenant_registry`` already swallows every failure, so there is
    nothing a caller could do with the handle. Garbage collection of a running
    task is prevented by the loop holding a strong reference until it completes.
    """
    global _last_demand_refresh

    hook = _refresh_hook
    if hook is None:
        return
    now = time.monotonic()
    if _last_demand_refresh is not None and now - _last_demand_refresh < (
        DEMAND_REFRESH_COOLDOWN_SECONDS
    ):
        return
    try:
        import asyncio

        loop = asyncio.get_running_loop()
    except Exception:
        return
    # Stamp BEFORE scheduling: if create_task raises, the cooldown still holds,
    # so a broken loop cannot be retried once per cost event.
    _last_demand_refresh = now
    try:
        loop.create_task(hook(), name="tenant_registry_demand_refresh")
    except Exception:  # pragma: no cover - a refresh must never break resolution
        logger.warning("tenant registry demand refresh could not be scheduled", exc_info=True)


#: What answered :func:`is_registered_tenant_id`. Read the counter's own help
#: text in ``observability.metrics`` for what each value licenses a reader to
#: conclude; the short form:
#:
#:   live              the LIVE registry overlay vouches for this id
#:   snapshot          only the checked-in snapshot does, and the overlay is
#:                     FRESH — so this is genuine drift (or a retired spelling),
#:                     not a degradation
#:   snapshot_degraded only the snapshot does, and the overlay is stale or was
#:                     never loaded — THE DEGRADATION. A sustained non-zero rate
#:                     here means billing is answering from the hand-pasted file
#:                     and precondition 2 is NOT met, whatever the code says.
#:   unknown           nothing vouches for it. This is the population the gate
#:                     would park.
def registration_source(canon: str) -> str:
    """Which registry answered for ``canon``. Pure: no counting, no I/O, no refresh.

    Split out from :func:`is_registered_tenant_id` so the verdict and the
    instrument are derived from ONE expression. A counter computed separately
    from the boolean it describes can disagree with it, and a disagreeing
    instrument is worse than none — it would be the evidence used to decide
    whether to arm the gate.

    ``"sentinel"`` is checked FIRST and therefore beats the live overlay. That
    order is the whole robustness of the control: the sentinel is a placeholder,
    not a customer, so no registry answer can make it one. Subtracting it from
    :data:`_REGISTERED_TENANT_IDS` alone would leave the verdict one licences
    refresh away from flipping back — if the upstream registry ever holds a row
    named ``system``, the overlay would vouch for …0001 and 1,207 rows/quarter
    would quietly resume billing under a tenant nobody owns.

    It is a source of its own rather than ``"unknown"`` for two reasons. The
    counter must be able to show the sentinel population separately from ids
    that are genuinely unknown — they call for different actions (fix the
    emitter vs. register the tenant). And ``"unknown"`` is what triggers
    :func:`_request_registry_refresh` in :func:`is_registered_tenant_id`; a
    refresh can never make the sentinel registered, so routing it there would
    ask licences a question whose answer is already known, once per event.
    """
    if canon in SENTINEL_TENANT_IDS:
        return "sentinel"
    if canon in _live_tenant_ids:
        return "live"
    if canon in _REGISTERED_TENANT_IDS or canon == NIL_TENANT_ID:
        return "snapshot" if live_registry_is_fresh() else "snapshot_degraded"
    return "unknown"


#: The :func:`registration_source` verdicts that mean "no registry vouches for
#: this id". Derived as a SET rather than restated as ``source != "unknown"`` at
#: the call site, so adding a source cannot silently default to "registered" —
#: which is the expensive direction, since a registered verdict bills.
_UNVOUCHED_SOURCES: frozenset[str] = frozenset({"unknown", "sentinel"})


def _count_registration_source(source: str) -> None:
    """Count one registration check by what answered. Never raises.

    Function-local import for the same circularity reason as
    :func:`_count_name_resolution`.
    """
    try:
        from observability.metrics import tenant_registration_source_total

        tenant_registration_source_total.labels(source=source).inc()
    except Exception:  # pragma: no cover - observability must not break auth
        pass


def set_live_tenant_registry(
    mapping: Mapping[str, str] | Iterable[tuple[str, str]],
) -> int:
    """Install a live registry snapshot as the overlay. Returns names installed.

    Accepts ``(name, uuid)`` PAIRS as well as a ``{name: uuid}`` mapping, and the
    production caller (``services.tenant_names.refresh_tenant_registry``) passes
    pairs on purpose: the registry is keyed by uuid, and folding it into a
    ``{name: uuid}`` dict first would silently last-one-win two tenants that
    share a name — exactly the case this function has to SEE to refuse it.

    Pure and synchronous on purpose — it does no I/O and knows nothing about
    HTTP. ``services.tenant_names.refresh_tenant_registry`` fetches; this
    installs. That split is what lets the resolver stay a sync function callable
    from pydantic validators and the stream flusher, and it keeps this module
    dependency-free.

    Refuses to install an EMPTY map and keeps the previous one. This is not
    tidiness: ``get_tenant_info`` returns ``{}`` for a 401, a timeout, a
    non-JSON body AND a genuinely empty registry, so an empty answer is
    indistinguishable from an outage. Clearing a good overlay on one is how a
    transient blip would silently become a resolution change.

    A value that does not parse as a uuid is DROPPED, not stored: the overlay
    exists to resolve names onto ids the registry vouches for, and installing an
    unparseable one would mint an identifier out of a payload we do not control.
    A name the payload maps to two different uuids is NOT resolvable: it is
    recorded in :data:`_live_ambiguous_names` and :func:`resolve_tenant_name_to_id`
    answers ``None`` for it (counted), never a pick — picking one would
    attribute somebody's spend to the wrong tenant. Both uuids still count as
    registered and keep their labels.
    """
    global _live_tenant_names, _live_tenant_ids, _live_refreshed_at, _live_tenant_labels
    global _live_ambiguous_names

    pairs: Iterable[tuple[object, object]] = (
        mapping.items() if isinstance(mapping, Mapping) else mapping
    )
    valid: list[tuple[str, str]] = []
    resolved: dict[str, str] = {}
    ambiguous: set[str] = set()
    for name, tenant_id in pairs:
        if not isinstance(name, str) or not name.strip():
            continue
        try:
            canon = str(uuid.UUID(str(tenant_id)))
        except ValueError, AttributeError, TypeError:
            continue
        key = name.strip()
        valid.append((key, canon))
        if key in resolved and resolved[key] != canon:
            ambiguous.add(key)
            continue
        resolved[key] = canon
    for key in sorted(ambiguous):
        resolved.pop(key, None)
        logger.warning(
            "live tenant registry lists %r on more than one tenant uuid (names are "
            "unique per owning app, not globally); a bare name will resolve to "
            "NO id — callers must send the uuid",
            key,
        )

    if not valid:
        return 0

    # Reverse view, built from the SAME validated pairs so the two directions are
    # one install, not two. Every valid pair contributes, including the ones
    # whose NAME is ambiguous: uuid -> name is still a single answer for each of
    # them. Ambiguity is dropped in THIS direction only when one uuid carries two
    # names (a rename the registry still lists under both spellings), which would
    # otherwise resolve by iteration order — a silently-varying answer to a
    # question about somebody's money.
    labels: dict[str, str] = {}
    label_ambiguous: set[str] = set()
    for name, canon in valid:
        if canon in labels and labels[canon] != name:
            label_ambiguous.add(canon)
            continue
        labels[canon] = name
    for canon in label_ambiguous:
        labels.pop(canon, None)
        logger.warning(
            "live tenant registry maps two different names onto uuid %s; dropping "
            "the reverse entry so the write path falls back to the uuid (counted "
            'on billing_placeholder_value_total{field="tenant_name"}) rather than '
            "labelling one tenant's spend with another's name",
            canon,
        )

    _live_tenant_names = resolved
    _live_tenant_ids = frozenset(canon for _, canon in valid)
    _live_tenant_labels = labels
    _live_ambiguous_names = frozenset(ambiguous)
    _live_refreshed_at = time.monotonic()
    # Same contract as :func:`_note_unregistered_tenant`: an observability
    # defect must never propagate out of the resolver's supply chain. Logged
    # rather than silently swallowed — this runs once per refresh, not per
    # request, so a warning here is affordable and a silent one would hide the
    # loss of the only drift instrument there is.
    try:
        _publish_registry_drift(resolved)
    except Exception:  # pragma: no cover - observability must not break auth
        logger.warning("tenant registry drift gauges not published", exc_info=True)
    return len(resolved)


def _publish_registry_drift(live: Mapping[str, str]) -> None:
    """Publish how far the checked-in snapshot has drifted from the registry.

    Computed at REFRESH time, not per request: it is O(registry) once every few
    minutes rather than on the hot path, and drift is a property of the two maps
    rather than of any one caller.

    ``absent_from_live`` is expected to be non-zero and equal to the declared
    retired spellings — it is reported, not alarmed on, because deleting those
    keys is the one remedy that would 400 live callers.
    """
    from observability.metrics import tenant_registry_drift, tenant_registry_live_entries

    missing = [n for n, tid in live.items() if LEGACY_TENANT_NAMES.get(n) != tid]
    conflicting = [
        n for n, tid in live.items() if n in LEGACY_TENANT_NAMES and LEGACY_TENANT_NAMES[n] != tid
    ]
    absent = sorted(set(LEGACY_TENANT_NAMES) - set(live))

    tenant_registry_live_entries.set(len(live))
    tenant_registry_drift.labels(kind="missing_from_snapshot").set(len(missing))
    tenant_registry_drift.labels(kind="conflicting_uuid").set(len(conflicting))
    tenant_registry_drift.labels(kind="absent_from_live").set(len(absent))

    if conflicting:
        # The only shape that resolved to the WRONG tenant rather than 400ing,
        # so it is the only one that warrants a warning.
        logger.warning(
            "tenant registry snapshot disagrees with the live registry on %d "
            "name(s) — the snapshot resolved them to a DIFFERENT tenant: %s",
            len(conflicting),
            sorted(conflicting),
        )
    if missing:
        logger.info(
            "live tenant registry holds %d name(s) the checked-in snapshot does "
            "not; regenerate tests/fixtures/licenses_tenants.txt: %s",
            len(missing),
            sorted(missing),
        )


def live_tenant_registry_stats() -> tuple[int, float]:
    """``(names_in_overlay, seconds_since_last_successful_refresh)``.

    Age is ``-1.0`` when no refresh has ever landed. For the refresh loop's
    gauge and for tests; the resolver itself never looks at the age (see
    property 1 above — the overlay is not expired).
    """
    if _live_refreshed_at is None:
        return len(_live_tenant_names), -1.0
    return len(_live_tenant_names), time.monotonic() - _live_refreshed_at


def write_gate_refuses_tenant(canon: str, *, max_overlay_age_s: float) -> bool:
    """True when a BUDGET WRITE for ``canon`` must be REFUSED, not recorded.

    ⚠️ THIS IS A DIFFERENT CONTROL FROM :func:`is_registered_tenant_id`, and the
    difference is the whole point. Read that function's ⛔ note first: it must
    FAIL OPEN, because it sits on the path a *money record* travels and refusing
    there silently drops live spend. This function sits on the path a *payer row*
    is created on. Refusing to invent a payer loses nothing — the caller is told,
    and the licence it was syncing is still there to retry once its tenant is
    registered. One boundary must never reject; the other must.

    WHY IT EXISTS, measured on prod 2026-09-10: ``token_budgets.tenant_id`` has
    no foreign key, ``services.budget_crud.create_budget`` inserted whatever it
    was handed, and ``upsert_license_sync_budget`` keeps ``tenant_id`` OUT of its
    idempotency key and so OVERWRITES a stored value with the caller's. The
    result was 61 live ``token_budgets`` rows whose ``tenant_id`` is actually a
    ``licenses.subject.id`` — a PERSON's primary key sitting in a column that
    means organisation — and whose own ``subject.tenant_id`` is NULL, so there is
    no owning tenant to repair them to. Nothing downstream can catch that: both
    ids are bare uuids on the wire and uuid VERSION is not a discriminator
    (genuine tenants are v4 too). The only place it is catchable is the write.

    ⛔ THE FRESHNESS GATE IS THE LOAD-BEARING HALF, NOT THE MEMBERSHIP TEST.
    ``canon not in registry`` has TWO causes that demand opposite verdicts:

      * the overlay is loaded and this id is genuinely absent  → REFUSE
      * the refresh loop has never succeeded, or last succeeded long ago, so the
        overlay is empty or stale and ABSENCE IS NOT EVIDENCE  → ALLOW

    Collapsing them would turn a licences-service outage into a refusal of every
    budget write in the estate, which is a far worse failure than the one this
    closes. So the answer is ``False`` — allow — whenever we cannot prove the
    registry was recently right. The checked-in ``_REGISTERED_TENANT_IDS``
    snapshot alone is NEVER sufficient proof: it was measured missing six real
    tenants on 2026-09-05, and an accusation built on a hand-maintained file is
    exactly the stale-allowlist shape that made arming the cost-flusher park gate
    unsafe.

    ``max_overlay_age_s`` is passed in rather than read from settings so this
    stays a pure function of (id, registry state, threshold) and the caller owns
    the policy. Derive it from ``settings.tenant_registry_refresh_seconds``; a
    multiple of the refresh interval means a single missed tick still permits a
    refusal while a genuine outage stops permitting one.
    """
    if is_registered_tenant_id(canon):
        return False
    _, age = live_tenant_registry_stats()
    # Refuse ONLY inside the proven-fresh window. ``age`` is -1.0 when no refresh
    # has ever landed, which falls outside it and therefore ALLOWS -- absence from
    # an overlay that was never loaded is not evidence of anything.
    return 0.0 <= age <= max_overlay_age_s


# ---------------------------------------------------------------------------
# The write gate's synchronous refresh on a miss
# ---------------------------------------------------------------------------
#
# ⛔ THE DEFECT (2026-10-13): CommonPractice creates a tenant in licences and
# writes its budget seconds later. The overlay that :func:`write_gate_refuses_tenant`
# consults is refreshed every ``tenant_registry_refresh_seconds`` (300s), and the
# demand refresh :func:`is_registered_tenant_id` schedules is fire-and-forget by
# design — so the verdict for THIS request was always the stale one. A tenant
# created seconds ago was refused ``budget_write_refused_unregistered_tenant``
# (422) for up to five minutes.
#
# The two controls have opposite failure contracts, and that is why this
# refresh is AWAITED here but must never be on the cost-event path: the write
# gate is allowed to wait (a payer row is created once, by an admin call), while
# the resolver must never block a money record on licences.
#
# Licences has no single-tenant lookup (only ``GET /internal/tenants`` — checked
# on licenses origin/main 2026-10-13), so a miss costs a FULL refresh. Hence the
# two rate limits, which bound what a flood of unknown ids can do to licences:
#
#   * per-id negative cache — an id that a refresh we ran did not find is not
#     asked about again for :data:`MISS_REFRESH_NEGATIVE_TTL_SECONDS`;
#   * global floor — no two miss refreshes start within
#     :data:`MISS_REFRESH_MIN_INTERVAL_SECONDS`; concurrent misses JOIN the one
#     in flight instead of starting another.
#
# So the worst case is one licences fetch per floor interval per replica,
# however many distinct ids arrive.
#
# ⛔ FAIL CLOSED: if the refresh errors, times out, or installs nothing, the
# overlay is unchanged and the gate's original refusal stands.

#: An id a refresh we ran did not find is refused WITHOUT another refresh for
#: this long. Same 30s as :data:`DEMAND_REFRESH_COOLDOWN_SECONDS`.
MISS_REFRESH_NEGATIVE_TTL_SECONDS: float = 30.0

#: Minimum seconds between the STARTS of two miss refreshes, across all ids.
MISS_REFRESH_MIN_INTERVAL_SECONDS: float = 5.0

#: Upper bound on how long a budget write waits for licences. The fetch itself
#: has a 3s httpx timeout; this also bounds the token mint in front of it.
MISS_REFRESH_TIMEOUT_SECONDS: float = 5.0

#: Bound on the negative cache, so a flood of random ids cannot grow memory.
MISS_NEGATIVE_CACHE_CAP: int = 1024

#: Clock for the miss-refresh state only. A seam for tests: patching
#: ``time.monotonic`` itself would also move the event loop's clock.
_monotonic: Callable[[], float] = time.monotonic

_miss_negative_cache: dict[str, float] = {}
_last_miss_refresh: float | None = None
_miss_refresh_inflight: asyncio.Task[int] | None = None


def _remember_miss(canon: str, now: float) -> None:
    """Record ``canon`` in the negative cache, keeping it under the cap."""
    _miss_negative_cache.pop(canon, None)
    if len(_miss_negative_cache) >= MISS_NEGATIVE_CACHE_CAP:
        for key in [
            k
            for k, ts in _miss_negative_cache.items()
            if now - ts >= MISS_REFRESH_NEGATIVE_TTL_SECONDS
        ]:
            del _miss_negative_cache[key]
        while len(_miss_negative_cache) >= MISS_NEGATIVE_CACHE_CAP:
            # dicts keep insertion order: drop the oldest entry.
            del _miss_negative_cache[next(iter(_miss_negative_cache))]
    _miss_negative_cache[canon] = now


async def _run_refresh_hook(hook: Callable[[], Any]) -> int:
    installed = await hook()
    return installed if isinstance(installed, int) else 0


async def _refresh_on_miss(canon: str) -> str:
    """Refresh the overlay for a write-gate miss, rate-limited. Never raises.

    Returns how it went: ``no_hook`` | ``negative_cached`` | ``rate_limited`` |
    ``refreshed`` (we ran a fetch that installed a map) | ``joined`` (we awaited
    a fetch already in flight, which installed a map) | ``failed``.
    """
    global _last_miss_refresh, _miss_refresh_inflight, _last_demand_refresh

    hook = _refresh_hook
    if hook is None:
        return "no_hook"
    now = _monotonic()
    seen = _miss_negative_cache.get(canon)
    if seen is not None and now - seen < MISS_REFRESH_NEGATIVE_TTL_SECONDS:
        return "negative_cached"

    loop = asyncio.get_running_loop()
    task = _miss_refresh_inflight
    if task is not None and not task.done() and task.get_loop() is loop:
        outcome = "joined"
    elif _last_miss_refresh is not None and now - _last_miss_refresh < (
        MISS_REFRESH_MIN_INTERVAL_SECONDS
    ):
        # The overlay is at most the floor interval old: re-reading it is the
        # answer a refresh would have given for any tenant created before then.
        return "rate_limited"
    else:
        # Stamp BEFORE the fetch so concurrent misses see the floor, and stamp
        # the demand-refresh cooldown too: the fire-and-forget refresh that
        # is_registered_tenant_id would otherwise schedule next is redundant.
        _last_miss_refresh = now
        _last_demand_refresh = time.monotonic()
        task = loop.create_task(_run_refresh_hook(hook), name="tenant_registry_miss_refresh")
        _miss_refresh_inflight = task
        outcome = "refreshed"

    try:
        installed = await asyncio.wait_for(asyncio.shield(task), MISS_REFRESH_TIMEOUT_SECONDS)
    except Exception:
        logger.warning("tenant registry refresh on a write-gate miss failed", exc_info=True)
        return "failed"
    if not installed:
        return "failed"
    if outcome == "refreshed" and canon not in _live_tenant_ids:
        _remember_miss(canon, now)
    return outcome


def _count_miss_refresh(result: str) -> None:
    """Count one write-gate miss refresh by result. Never raises."""
    try:
        from observability.metrics import tenant_registry_miss_refresh_total

        tenant_registry_miss_refresh_total.labels(result=result).inc()
    except Exception:  # pragma: no cover - observability must not break the gate
        pass


async def write_gate_refuses_tenant_after_refresh(canon: str, *, max_overlay_age_s: float) -> bool:
    """:func:`write_gate_refuses_tenant`, but a REFUSAL is first re-checked live.

    Only an id nothing vouches for, inside the proven-fresh window, can be
    refused — so only that case pays for a refresh. Every other verdict is
    returned exactly as :func:`write_gate_refuses_tenant` gives it. After the
    refresh the verdict is recomputed against the new overlay; if the refresh
    did not land, the overlay is unchanged and the refusal stands (fail closed).
    """
    _, age = live_tenant_registry_stats()
    would_refuse = registration_source(canon) == "unknown" and 0.0 <= age <= max_overlay_age_s
    if not would_refuse:
        return write_gate_refuses_tenant(canon, max_overlay_age_s=max_overlay_age_s)
    outcome = await _refresh_on_miss(canon)
    refused = write_gate_refuses_tenant(canon, max_overlay_age_s=max_overlay_age_s)
    if outcome in ("failed", "no_hook", "negative_cached"):
        _count_miss_refresh(outcome)
    elif not refused:
        _count_miss_refresh("found")
    else:
        _count_miss_refresh("rate_limited" if outcome == "rate_limited" else "absent")
    return refused


def reset_live_tenant_registry() -> None:
    """Drop the overlay so resolution falls back to the snapshot. For tests.

    Also drops the demand-refresh hook and its cooldown stamp: a test that
    installed a hook must not leave it scheduling tasks onto another test's
    event loop, and a stamp left behind would silently suppress the next test's
    refresh request.
    """
    global _live_tenant_names, _live_tenant_ids, _live_refreshed_at, _live_tenant_labels
    global _live_ambiguous_names, _refresh_hook, _last_demand_refresh
    global _last_miss_refresh, _miss_refresh_inflight
    _live_tenant_names = {}
    _live_tenant_ids = frozenset()
    _live_tenant_labels = {}
    _live_ambiguous_names = frozenset()
    _live_refreshed_at = None
    _refresh_hook = None
    _last_demand_refresh = None
    _miss_negative_cache.clear()
    _last_miss_refresh = None
    _miss_refresh_inflight = None


def live_tenant_label(tenant_id: Any) -> str | None:
    """Resolve a tenant uuid to the registry's NAME, or ``None``. Never raises.

    The reverse of :func:`resolve_tenant_name_to_id`, and the WRITE-path
    counterpart of :func:`services.tenant_names.resolve_tenant_name` — same
    registry, same refresh, so a label stored on a row and a label rendered on
    the dashboard cannot disagree about what the registry said.

    THREE PROPERTIES, all load-bearing, because this is called once per cost
    event on the Redis-stream flush path:

    1. PURE AND SYNCHRONOUS. A dict lookup on an overlay some other task keeps
       fresh. No I/O, no await, no lock — the flusher must not acquire a licences
       dependency, and ``services.tenant_names.get_tenant_names`` (which does
       HTTP behind a TTL cache) is therefore the wrong door from here.
    2. NEVER RAISES, NEVER BLOCKS. A registry miss returns ``None`` and the
       caller keeps its existing fallback. Throwing here would fail a money
       record over a display label, which is far worse than storing a uuid.
       Wrapped anyway: this is the hot path, and a future edit that makes the
       overlay something other than a plain dict must not be able to break it.
    3. NEVER FABRICATES. Only a name the registry actually vouches for is
       returned. An unknown tenant, an empty overlay (no refresh has landed, or
       licences is down) and an ambiguous uuid all answer ``None`` — the caller
       then stores what it stored before and COUNTS it, so "the registry did not
       know" stays distinguishable from "the registry said so".
    """
    try:
        return _live_tenant_labels.get(str(tenant_id)) or None
    except Exception:  # pragma: no cover - a label must never break the flush
        return None


def _note_ambiguous_name(name: str) -> None:
    """Warn and count one ambiguous-name resolution. Never raises.

    The label is bounded: only names already in :data:`_live_ambiguous_names`
    (i.e. listed by the registry itself on two or more tenants) reach here.
    """
    try:
        from observability.metrics import tenant_name_ambiguous_total

        tenant_name_ambiguous_total.labels(name=name).inc()
    except Exception:  # pragma: no cover - observability must not break auth
        pass
    logger.warning(
        "tenant name %r matches more than one live licences tenant; "
        "resolved to NO id — the caller must send the tenant uuid",
        name,
    )


def resolve_tenant_name_to_id(name: str) -> str | None:
    """Resolve a tenant NAME through the live overlay, then the snapshot.

    Named ``..._to_id`` rather than ``resolve_tenant_name`` because
    :func:`services.tenant_names.resolve_tenant_name` already exists and runs the
    OPPOSITE direction (uuid -> display label). Two same-named functions
    resolving in opposite directions is a mis-import waiting to happen.

    Exact-match on both, deliberately: ``LEGACY_TENANT_NAMES`` has always been
    exact-match, and making the live map case-insensitive would widen what this
    service accepts as a side effect of a reliability change.

    ⛔ A NAME THE LIVE REGISTRY LISTS ON MORE THAN ONE TENANT RESOLVES TO
    ``None``. Licences tenant names are unique per owning app, not globally, so
    such a name does not identify a tenant. It does not fall through to the
    snapshot: the snapshot's uuid would be a pick as well. Logged and counted on
    ``billing_tenant_name_ambiguous_total{name}`` so the caller can be found and
    moved to uuids.
    """
    if name in _live_ambiguous_names:
        _count_name_resolution("ambiguous")
        _note_ambiguous_name(name)
        return None
    live = _live_tenant_names.get(name)
    if live is not None:
        _count_name_resolution("live")
        return live
    snapshot = LEGACY_TENANT_NAMES.get(name)
    if snapshot is not None:
        _count_name_resolution("snapshot")
        return snapshot
    _count_name_resolution("unknown")
    return None


def looks_like_credential(s: str) -> bool:
    """True if the value looks like a leaked auth header/token, not a tenant id.

    Lives here rather than in ``services/_parsers`` so ONE definition serves both
    the flusher (which must never persist a secret in a ``tenant_id`` column) and
    the HTTP boundary (which must never echo one back in an error body or a log
    line). Two copies would drift, and the copy that drifts is the one that leaks.

    Observed in production: four rows arrived with a raw ``Bearer <jwt>`` in the
    ``X-Tenant-Id`` header, so this is a measured shape, not a hypothetical one.
    """
    low = s.strip().lower()
    return (
        low.startswith(("bearer", "basic ", "token "))
        or len(s) > 48
        or any(ws in s for ws in (" ", "\t", "\n"))
    )


def redact_tenant_id(raw: Any) -> str:
    """Render a bad tenant_id for logs without leaking secrets.

    Short slugs ("n8n-dev", "unknown") are safe to show in full; long or
    Bearer-prefixed values (raw auth headers) are truncated so a token never
    lands in the logs. Single definition, shared by the flushers and by
    :mod:`auth_deps` — the HTTP boundary rejects exactly the values the flusher
    quarantines, so both must redact them identically.
    """
    s = str(raw)
    if len(s) > 24 or s.lower().startswith("bearer"):
        return f"{s[:8]}…(redacted,len={len(s)})"
    return s


def is_registered_tenant_id(canon: str) -> bool:
    """True if ``canon`` is a uuid the licences-registry snapshot vouches for.

    The PUBLIC form of the check :func:`canonical_tenant_id` already makes
    internally. It is exported so the cost flusher can act on the answer instead
    of only counting it: resolution FAILS OPEN (see
    :func:`_note_unregistered_tenant` — rejecting before the denominator is
    measured silently drops live spend), so the id is returned either way, and
    the flusher needs to know whether to park the resulting row on
    ``excluded_from_billing``.

    ⛔ DO NOT turn this into a rejection at the resolution boundary. The money
    record must survive; what must not survive is the row counting toward a
    billable total. Those are different controls and only the second one is
    safe to arm before the first is measured.

    ⚠️ R15: :func:`canonical_tenant_id` NOW CAN reject at the resolution
    boundary, but that capability does not live here and is not this
    function's default — it is a separate, explicitly-armed setting
    (:attr:`settings.Settings.billing_tenant_id_classification_enforce`,
    default ``False``) layered on top of :func:`classify_tenant_id`, which
    calls this function rather than replacing it. This function's own verdict
    is unchanged: it still only ever answers a boolean and never raises,
    rejects, or blocks.

    The nil uuid answers ``True``: it IS a registered row in
    ``licenses.tenants`` (``default-anon``), and an event that resolves to it is
    already dead-lettered by :func:`services._cost_schema.validate_cost_event`
    long before this could be consulted. Answering ``False`` for it would mean
    routing an event to two different controls for one condition.

    A SENTINEL uuid answers ``False``, and it is the one verdict no registry can
    overturn — see :data:`SENTINEL_TENANT_IDS` and :func:`registration_source`.
    ``"system"`` still RESOLVES by name, so the event passes validation and
    reaches the flusher with its money intact; it simply arrives unvouched-for
    and is parked on ``excluded_from_billing`` instead of billed. Making it
    unresolvable instead would dead-letter it and destroy the record, which is
    the trade this docstring's ⛔ above refuses.

    The LIVE overlay's ids are UNIONED in, never substituted for the snapshot's.
    Union is the only safe direction: a live refresh can then make this
    accusation RARER (the six registered names the snapshot was missing on
    2026-09-05 stop being called unregistered) but never more common, so a
    licences blip cannot start parking real rows on ``excluded_from_billing``.

    ⭐ IT IS A LIVE CONSULT NOW, AND IT SAYS SO ON A COUNTER. Every call reports
    what answered on ``billing_tenant_registration_source_total{source}`` (see
    :func:`registration_source`), which is the control that can FAIL: without it,
    "the live registry is answering" and "the overlay has been empty since boot"
    are the same green. An id nothing vouches for additionally asks for a refresh
    OFF this path (:func:`_request_registry_refresh`), so a tenant registered
    since the last tick is picked up in seconds rather than at the next
    interval — but the verdict returned HERE is still the one the overlay
    already held. Nothing blocks, nothing awaits, nothing can raise.
    """
    source = registration_source(canon)
    _count_registration_source(source)
    if source == "unknown":
        _request_registry_refresh()
    return source not in _UNVOUCHED_SOURCES


def canonical_tenant_id(raw: Any) -> str | None:
    """Resolve any tenant identifier to its lowercase canonical uuid string.

    Returns ``None`` when the value cannot be resolved — deliberately, so the
    caller must decide what an unknown tenant means (a 400 at the HTTP boundary,
    the nil tenant in a background flusher). Returning a fabricated id here is
    what produced the ``"unknown"`` pseudo-tenant.

    Resolution order:

    a. anything :class:`uuid.UUID` accepts → ``str(uuid.UUID(raw))``. Parsing is
       delegated to the stdlib on purpose: it already accepts the braced form,
       the ``urn:uuid:`` form and the 32-char dashless form, and it normalises
       case. A hand-rolled ``[0-9a-f]{8}-…`` regex would misclassify a valid but
       non-canonical uuid as garbage and 400 a legitimate caller.
    b. an exact key of the LIVE registry overlay → the uuid the licences
       registry currently holds for it. See :func:`set_live_tenant_registry`.
    c. an exact key of :data:`LEGACY_TENANT_NAMES` → its registered uuid. This
       is the FAIL-SOFT path: with no successful refresh it answers everything,
       which is byte-for-byte the behaviour that shipped before the overlay
       existed. The name is resolved, never stored.
    d. anything else → ``None``. An unknown name is still never minted.

    Step (a) applies NO registry check: any well-formed uuid is accepted as a
    tenant. That is deliberately left in place — an unregistered id is counted on
    ``billing_tenant_unregistered_total`` and logged once, then returned. See
    :func:`_note_unregistered_tenant` for why this fails open rather than
    rejecting.

    THE ACCEPT PATH IS NO LONGER THE END OF THE STORY. Returning the id was, on
    its own, fail-open with no downstream consequence: any well-formed uuid was
    billed as a tenant the licences registry has never heard of. The gap is now
    closed WITHOUT rejecting anything, one layer down — ``services.cost_flusher``
    asks :func:`is_registered_tenant_id` about the resolved id and persists the
    row with ``excluded_from_billing = true`` and
    ``exclusion_reason = 'unregistered_tenant'``. The money record survives and
    stays auditable (every aggregate already filters on that column); it simply
    stops counting toward a billable total, and the volume is readable on
    ``billing_cost_events_excluded_total{reason="unregistered_tenant"}``.

    ⛔ THAT REMAINS THE DEFAULT, AND IT IS NOT THIS DOCSTRING'S CALL TO CHANGE
    IT. Rejecting HERE dead-letters the event instead of storing it, which
    destroys the money record — strictly worse than mis-attributing it, and
    unrecoverable, because billing is append-only with no refund path. Refusing
    unregistered production spend by default could refuse REAL revenue, and
    that is an operator's decision, not an engineering default.

    ⭐ R15 ESTATE REPAIR (2026-09-16) adds a SECOND, OFF-BY-DEFAULT mode rather
    than changing the default. Every id accepted by step (a) is now run through
    :func:`classify_tenant_id` — which subsumes the old bare
    :func:`is_registered_tenant_id` check and additionally names WHAT KIND of
    unregistered id it is (a leftover test fixture, a name-derived v5 with no
    owner, a stray v7/v4) — and the verdict is counted on
    ``billing_tenant_id_classification_total{classification}``, pre-seeded at
    zero for every member of :data:`TENANT_ID_CLASSIFICATIONS` so a reader can
    tell "never happened" from "the instrument is not wired". ONLY when
    :attr:`settings.Settings.billing_tenant_id_classification_enforce` is
    explicitly armed (default ``False``) does a non-``"registered"``
    classification make this function return ``None`` instead of ``canon`` —
    counted separately on
    ``billing_tenant_id_classification_rejected_total{classification}`` so the
    volume a future arming would refuse is measurable **before** anyone arms
    it, the same "do not close a gate before its counter reads 0" discipline
    :data:`settings.Settings.billing_exclude_unregistered_tenants` already
    documents. In the default (disarmed) state this function is
    behaviour-identical to before this mode existed: it still returns ``canon``
    for every well-formed uuid, registered or not.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        canon = str(uuid.UUID(s))
    except ValueError, AttributeError, TypeError:
        pass
    else:
        classification = classify_tenant_id(canon)
        _count_tenant_id_classification(classification)
        if classification != "registered":
            _note_unregistered_tenant(canon)
            if _tenant_id_classification_enforced():
                _count_tenant_id_classification_rejected(classification)
                return None
        return canon
    return resolve_tenant_name_to_id(s)


def tenant_level_project_id(tenant_id: Any) -> str | None:
    """The project id a PROJECT-LESS app sends for a tenant's own spend.

    ⭐ OWNER RULING 2026-10-07: apps with no project concept (core_app,
    worker_management) send ``X-Project-Id = tenant_level_project_id(tenant)``:
    the 32 lowercase hex chars of the canonical uuid (no hyphens), REVERSED,
    re-formatted as a canonical 8-4-4-4-12 uuid. It is deliberately NOT the raw
    tenant id -- ``project_id == tenant_id`` stays a forged-project signal
    (``BillingProjectIdEqualsTenant``) and is still refused at ingest.

    Fixed vector (callers must agree byte for byte):
    ``0195f48b-d000-7000-8000-000000000100`` ->
    ``00100000-0000-0008-0007-000db84f5910``.

    An involution: applying it twice returns the input, which is what lets
    ingest map an asserted tenant-level project id back to the ONE tenant it
    belongs to without any lookup table. Returns ``None`` for input that is not
    a uuid; never raises.
    """
    try:
        hex32 = uuid.UUID(str(tenant_id).strip()).hex
    except ValueError, AttributeError, TypeError:
        return None
    return str(uuid.UUID(hex=hex32[::-1]))


def derived_tenant_id(legacy: str) -> str:
    """Derive a stable uuid for a legacy tenant name.

    uuid5, not uuid7: a v7 uuid embeds the millisecond it was minted, which would
    assert that a tenant that has existed for a long time was created the moment
    the migration ran. There is no honest mint time for a value that was already
    in the table, so the id is made a pure deterministic function of the name
    instead — the same name always derives the same uuid, on any host, in any
    replay of the migration.
    """
    return str(uuid.uuid5(TENANT_NAMESPACE, legacy))
