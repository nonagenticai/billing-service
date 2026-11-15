# The unregistered-tenant gate, and the 403 underneath it

**Status: ARMED in production since 2026-09-16** (`BILLING_EXCLUDE_UNREGISTERED_TENANTS=true`,
gitops `clusters/strato/services/billing/deployment.yaml`, gitops #3906). **The code default is
`true` since 2026-10-15**, so every deployment parks unless it sets
`BILLING_EXCLUDE_UNREGISTERED_TENANTS=false` (that is how to disarm). Evidence for flipping the
default, measured in prod: over 7d `billing_tenant_registration_source_total` read
`live`=47,238, `snapshot_degraded`=0, `snapshot`=1; over 14d
`billing_cost_events_unregistered_tenant_total` read 0.

> ⚠️ **CORRECTED 2026-10-15 — this line said "shipped OFF … measuring only" for a month after the
> gate was armed.** Verified 2026-10-15 from the live Deployment and a running pod's env. ⚠️ Do
> not read the gate's counters as evidence it is behaving yet: the estate moved to separated
> per-app licences on 2026-10-14 and almost no app traffic has flowed since, so zeros on
> `billing_cost_events_unregistered_tenant_total` mean "no traffic", not "no unregistered tenants".
> Sections below titled *Why it shipped OFF* and *Arming criterion* are the pre-arming record — kept
> because they are still the disarm/re-arm reasoning, not current state.
>
> The stricter sibling `BILLING_TENANT_ID_CLASSIFICATION_ENFORCE` (reject instead of park) is
> **off, deliberately**: rejecting dead-letters spend with no un-park path, and the park already
> keeps it out of billing. Review its shadow counter
> `billing_tenant_id_classification_rejected_total` by class over real traffic before any change.

## What the gate does

`tenancy.canonical_tenant_id` resolves step (a) — "anything `uuid.UUID` accepts" — with **no
registry lookup at all**. It returns the id unchecked, so a caller can mint a tenant simply by
putting a random uuid in `X-Tenant-Id` and be metered as one. `_cost_schema.validate_cost_event`
does not catch this either: it rejects a tenant that fails to *resolve*, and an unregistered uuid
resolves perfectly.

The gate does **not** reject such an event — dead-lettering would destroy the money record, which
is worse than mis-attributing it and is unrecoverable, because billing is append-only with no
refund path. Instead, when armed, `services.cost_flusher` **parks** the row:

- `excluded_from_billing = true`, `exclusion_reason = 'unregistered_tenant'`
- withheld from the token rollup and the budget debit
- still stored, still auditable, reversible with one `UPDATE`

When **disarmed** (`BILLING_EXCLUDE_UNREGISTERED_TENANTS=false`; the code default is armed) it
does none of that and only increments
`billing_cost_events_unregistered_tenant_total{service_key, action="counted"}`.

## ⛔ Why it shipped OFF (pre-arming record)

> ⚠️ **CORRECTED 2026-09-13 — this section used to open "Billing has no live view of the tenant
> registry… HTTP 403 on `GET /tenants` (measured 2026-08-28)". That is STALE and the conclusion it
> licenses is wrong; do not reason from it.** Billing reaches the registry through a *different*
> endpoint, `GET {licenses_url}/internal/tenants` (licenses #174/#177), on a 5-minute
> `run_tenant_registry_refresh_loop` that runs on every replica and installs a live NAME → uuid
> overlay via `tenancy.set_live_tenant_registry`. Measured live on the leader replica 2026-09-13:
> `billing_tenant_registry_refresh_total{result="ok"} = 115`, `{result="error"} = 0`,
> refresh age ~0.0002s, and the refresh log reads `names: 76` — **exactly** the 76 rows in
> `licenses.tenants`, archived ones included (the overlay deliberately passes
> `include_archived`, so an archived-but-real tenant is *not* mistaken for a ghost).
> `tenancy.is_registered_tenant_id` unions that live overlay with the checked-in snapshot, so the
> snapshot is now a **fallback**, not the only input. The paragraphs below still describe the
> snapshot's failure mode correctly, and it remains the answer during a licences outage — but
> "billing cannot see the registry" is no longer true.

The staleness argument that follows applies to the **fallback** path — what answers when the live
refresh degrades to `{}` (token misconfiguration, expired secret, licences outage). It is still the
reason the gate is not armed, because a gate must not close on input it cannot trust *at its worst*.

> ⚠️ **SUPERSEDED 2026-09-16 for the paragraph below.** "The only thing billing can check a tenant
> against is the snapshot" stopped being true when the registration check itself started reading the
> live overlay first — see **Precondition 2 is now met by code**. The snapshot is the FALLBACK. The
> failure mode it describes is still exactly right *about the fallback*, which is why it stays here.

Until 2026-09-16 the only thing billing could check a tenant against was
`tenancy._REGISTERED_TENANT_IDS` — a **hand-maintained snapshot** pasted in from a manual `psql`
query against the prod `licenses` database. That snapshot was the gate's entire input, and it is
unreliable by construction:

> Measured 2026-09-05: the snapshot was **11 days old** and missing **six genuinely registered
> tenant names** — `claude`, `procedure-improvement`, `prompt-improvement`, `svc-kag`,
> `svc-kag-eval-r46`, `svc-w2v-shared`. Every caller sending one was taking a hard **400** from
> this service, and had the gate been armed on merge their spend would additionally have been
> parked as "unregistered" and silently **under-billed**.

That is a guard failing in the expensive direction on input it cannot trust. Hence the estate rule
this follows: **do not close a gate before its counter reads 0** — the same discipline as
`INFERENCE_ROUTER_REQUIRE_CALLER_LICENSE` / `WORKER_MANAGEMENT_REQUIRE_AUTHENTICATED_TENANT`
across the fleet, both of which are also deliberately shipped off.

**The snapshot is a workaround. The 403 was the defect — and it is fixed.**

## Precondition 2 is now met by code (2026-09-16)

The three follow-ups this section used to list as "not attempted" are done. Item (1) — read access
for billing's service identity — turned out not to need a licences-side change at all: billing
already authenticates to `GET {licenses_url}/internal/tenants` with its own `ServiceTokenProvider`,
verified live from inside pod `billing-service-5d65849c54-rng6d` on 2026-09-16 (**200, 59 rows**;
unauthenticated the same call is **401**, which is the negative control). The route is
`/internal/tenants`; `/api/tenants` does not exist.

What this change added, in billing only:

1. **The registration check reads the live registry first.** `tenancy.is_registered_tenant_id` —
   the check `services.cost_flusher` builds the gate on — now resolves through
   `tenancy.registration_source`, which consults the live overlay before the checked-in snapshot. A
   tenant registered since the last paste is vouched for; it previously was not, which is precisely
   how the six missing names of 2026-09-05 would have been parked.
2. **Cached, never per-event HTTP.** The overlay is an in-memory map refreshed by
   `services.tenant_names.run_tenant_registry_refresh_loop` every
   `TENANT_REGISTRY_REFRESH_SECONDS` (**default 300s**, matching the 300s `TTLCache` on the fetch
   itself, so a tick is a real fetch and not a re-read). The hot path does **zero** I/O. The window
   *between* ticks is closed by a demand refresh: an id that **nothing** vouches for asks for a
   refresh off the request path (`asyncio.create_task`, never awaited), rate-limited to one per
   `tenancy.DEMAND_REFRESH_COOLDOWN_SECONDS` (**30s**, the same floor the loop already applies), so
   a caller spraying random uuids cannot turn this into one licences request per cost event.
3. **Fail safe, not closed.** Unreachable licences, a 401, a timeout, a shape change, or no refresh
   having ever landed all degrade to the snapshot, exactly as before — and the degradation is
   *counted*, not silent. `tenancy.live_registry_is_fresh()` treats an overlay older than
   `REGISTRY_STALE_AFTER_SECONDS` (**900s = 3× the refresh interval**, the same multiple
   `BUDGET_WRITE_REQUIRES_REGISTERED_TENANT` already uses) as no evidence at all. Nothing here can
   raise, block, or change a verdict.

**The instrument — this is the part that can fail, and the reason the change is checkable:**

`billing_tenant_registration_source_total{source}`, incremented once per registration check:

| `source` | what it means |
|---|---|
| `live` | the live registry overlay vouched for the id — **the proof the live path is being used** |
| `snapshot` | only the checked-in snapshot did, while the overlay is FRESH — ordinary drift, or a retired spelling |
| `snapshot_degraded` | only the snapshot did, and the overlay is stale or was never loaded — **the degradation** |
| `unknown` | nothing vouched for it — the population the gate would park |

⛔ **Code that consults the registry live is not evidence that it IS consulting it.** A token
misconfiguration falls back to the hand-pasted file forever, in silence, with the service still
green — which is why arming must be gated on a scrape, not on this document. **After deploy, read
the counter:** a healthy `live` rate with `snapshot_degraded` at or near zero is precondition 2
satisfied *in production*. A sustained `snapshot_degraded` rate means billing is answering out of
the snapshot and precondition 2 is **not** met, whatever the code says.

What is still true: CI cannot see the registry, so `tests/test_tenant_registry_shim.py` still
compares the snapshot against a checked-in fixture refreshed by
`scripts/refresh_tenant_registry_fixture.sh`, and **nothing in CI can catch registry↔snapshot
drift**. That matters less than it did — the snapshot is no longer the only input — but the
fallback it feeds is still hand-maintained.

**The gate itself is untouched.** `BILLING_EXCLUDE_UNREGISTERED_TENANTS` remains `false` (its
default, and still absent from the live Deployment). This change makes precondition 2 *satisfiable*;
it does not arm anything. *(Record as of 2026-09-16. Armed in prod the same day; code default
flipped to `true` 2026-10-15 — see Status at the top.)*

## Which tenants — the named series

Counting the population is rung 1 and it is **all this is**; nothing here rejects anything.

`services.unregistered_tenant_spend` re-derives the population from the **ledger** on every leader
tick and publishes four series:

| metric | shape |
|---|---|
| `billing_unregistered_tenant_spend_usd` | total USD, unlabelled — authoritative |
| `billing_unregistered_tenant_count` | distinct offending ids, unlabelled |
| `billing_unregistered_spend_by_tenant_usd{tenant_id}` | **named** USD, capped |
| `billing_unregistered_events_by_tenant{tenant_id}` | **named** event count, capped |

⛔ **Aggregate with `max()`, never `sum()`** — only the rollup leader publishes.

The two named series are capped at `MAX_NAMED_TENANT_SERIES` (20) ids **ordered by spend
descending**, with the residual folded into a single `tenant_id="__overflow__"` bucket and counted
by `billing_unregistered_tenant_series_dropped_total`. `tenant_id` is caller-supplied, so its label
domain is open; the cap is what stops a minting emitter turning `/metrics` into a cardinality
incident. Ordering by **money** is what makes the cap degrade safely — a flood of freshly minted
uuids is by construction low-spend and sorts into `__overflow__`, leaving the expensive ghosts
named. **A non-zero drop counter means fix the emitter, not raise the cap.**

Series are `clear()`ed and re-derived every pass, so a tenant that gets registered **stops
existing** rather than freezing at its last value — otherwise the alert this metric raises could
never resolve.

Measured live 2026-09-13: 4 ids, 932 events, **$23.203075** — `ad976f5b…`, `6c6a0da6…`,
`0195f48b-…-01f2`, `351fdc59…`.

## Arming criterion

Both conditions, not either:

1. **`billing_cost_events_unregistered_tenant_total{action="counted"}` reads 0** over a
   representative window — or names only `service_key`s whose tenants have been *confirmed* absent
   from the licences registry. A non-zero value that has not been explained is a reason not to arm.
2. **The snapshot-staleness root cause above is fixed** — billing can consult the registry live, or
   the refresh is automated and verified. Arming against a stale snapshot re-creates the exact
   defect this document exists to prevent.
   > **Status 2026-09-16: met by code, pending one scrape to be met in fact.** The registration
   > check consults the live registry first and falls back to the snapshot (see *Precondition 2 is
   > now met by code*). The word "verified" in this criterion is deliberate and is **not** satisfied
   > by the code landing: verify it against
   > `billing_tenant_registration_source_total{source="live"}` rising while
   > `{source="snapshot_degraded"}` stays flat, on a deployed replica, over a representative window.

Age of the flag is not evidence. If it has been off for months and the counter is still non-zero,
that is the counter doing its job, not a stale flag.

**To arm:** set `BILLING_EXCLUDE_UNREGISTERED_TENANTS=true` in the Deployment env block.
**To reverse a park after arming:**

```sql
UPDATE billing_cost_events
   SET excluded_from_billing = false, exclusion_reason = NULL
 WHERE exclusion_reason = 'unregistered_tenant';
```
