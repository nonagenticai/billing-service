# Rate-card proposal — numbers for the operator to set

**Status: PROPOSAL. Nothing here is applied.** No migration, no seed, no row. This
document exists because the operator asked for a starting scheme derived from measured
spend rather than invented, and because three tables being empty is the only thing
standing between this service and invoicing.

Measured **2026-09-10** against the production database on worker4. Every figure is
reproducible from the query beside it.

---

## ⛔ Read this first: there is no customer to bill yet

Across the **last 30 days of spend there is not one `kind='customer'` tenant.** Every
billed tenant is `internal` or `test`:

| tenant | kind | 30d spend |
|---|---|---|
| `n8n-dev-portal-realm` | internal | $2,516 |
| `n8n-dev` | internal | $1,243 |
| `portal-render-e2e` | internal | $642 |
| `aa-agents` | internal | $361 |
| `fr-test-user` | **test** | $138 |
| `platform-core` | internal | $99 |
| `system` | internal | $25 |

So the empty rate tables are **not currently losing money** — they cannot under-bill a
customer who does not exist. What they would do is let the FIRST customer be onboarded
against a $0.00 rate card. That is the risk being closed here, and it is a
before-the-fact risk, not an outstanding loss.

⚠️ This also means a rate card cannot be validated against revenue yet. Applying these
numbers today bills internal cost centres against each other.

---

## 1. `billing_app_cost_rates` — ⛔ DO NOT FILL THIS YET. Fix one thing first.

This is the table PR #171's close gate refuses on, so it looks like the urgent one. It
is not, and the reason is worth the paragraph.

The gate (`invoices.py:assert_all_service_keys_priced`) reads **`billing_metering_daily`
— API REQUEST rollups — not cost events**, excludes the system/S2S licence sentinel, and
excludes keys with `request_count = 0`. Apply exactly those filters and the entire set
of keys that could ever block a close is **five rows**:

```sql
SELECT service_key, count(DISTINCT tenant_id) tenants, sum(request_count) reqs,
       min(bucket_date), max(bucket_date)
FROM billing_metering_daily
WHERE license_id <> '00000000-0000-0000-0000-000000000000' AND request_count > 0
GROUP BY 1 ORDER BY 3 DESC;
```

| service_key | tenants | requests | first | last |
|---|---|---|---|---|
| **`unmapped:api`** | 3 | 92 | 2026-09-05 | 2026-09-09 |
| `workers` | 1 | 47 | 2026-09-05 | 2026-09-07 |
| **`unmapped:autonomy`** | 1 | 13 | 2026-09-06 | 2026-09-09 |
| `api` | 1 | 10 | 2026-09-05 | 2026-09-05 |
| `autonomy` | 1 | 4 | 2026-09-05 | 2026-09-05 |

(108 of the 124 rows carry the system sentinel and are excluded by the gate.)

⛔ **Two of the five are `unmapped:*`, which is a DEFECT PLACEHOLDER, not a service.**
`consumer.py` files a usage event under `unmapped:<segment>` when its path matches no
prefix in `SERVICE_KEY_MAP` *and* its payload names no emitter. It is live and moving:
`billing_service_key_unmapped_total` reads **681 and 741** on the two replicas.
**Pricing `unmapped:api` would put a defect on an immutable invoice line.**

⛔ **And do NOT "fix" it by adding `/api` to `SERVICE_KEY_MAP`.** `/api` is shared —
fr mounts `/api/<resource>`, karma `/api/karma`, kag `/api/documents` — so a prefix rule
would attribute all of them to whichever service won the mapping. That is precisely the
defect this code already fixed once: the old miss branch returned the first path segment
verbatim and **invented a service called `api`**, filing ~2,733 events/day of fr's
traffic under it. The `api` and `autonomy` rows above, both last seen 2026-09-05, are
the residue of that behaviour.

**The real fault is upstream and in another repo: those emitters do not name
themselves.** The fix is for the caller to send its emitter identity, after which the
honest keys appear and can be priced. Until then:

| action | why |
|---|---|
| price `workers` | a real, mapped service key |
| **leave `unmapped:*` unpriced** | #171's gate then REFUSES the close — which is correct. A refusal naming an unmapped key is the defect surfacing, exactly what the gate is for |
| do not price `api` / `autonomy` | residue of a fixed defect; no traffic since 2026-09-05 |

⚠️ So the first close attempt SHOULD fail, and that failure is the finding. Do not make
it pass by inventing rates.

---

## 2. `provider_pricing` already prices the cost side — check before adding rows

A genuine surprise worth recording, because it almost produced duplicate rate rows.
Per-request cost derived from 30 days of `billing_cost_daily` matches
`provider_pricing.per_request_usd` **exactly**:

| service | measured $/request | `provider_pricing.per_request_usd` |
|---|---|---|
| `karma` | 0.02307692 | **0.02307692** |
| `hirag` | 0.06153846 | **0.06153846** |
| `kag` | 0.01153846 | **0.01153846** |
| `kag-graph` | 0.01153846 | **0.01153846** |
| `w2v-image` | 0.01000000 | **0.01000000** (`mflux`) |

```sql
SELECT service_key, sum(request_count) reqs, round(sum(total_cost_usd),4) usd,
       round((sum(total_cost_usd)/nullif(sum(request_count),0))::numeric,8) usd_per_req
FROM billing_cost_daily WHERE bucket_date >= current_date - 30
GROUP BY 1 HAVING sum(request_count) > 0 ORDER BY 3 DESC;
```

⭐ **`billing_app_cost_rates` and `provider_pricing` are different questions and must not
be reconciled to each other.** `provider_pricing` prices what a unit of PROVIDER work
cost (105 effective rows, 104 `cash` / 51 `internal_transfer`). `billing_app_cost_rates`
prices an API REQUEST to one of our apps. A service can legitimately appear in one and
not the other. Cost coverage is already clean: **0 of 23 (provider, model) pairs are
unpriced.**

Full 30-day cost picture, for sizing only:

| service_key | requests | 30d $ | $/request |
|---|---|---|---|
| `inference-router` | 251,087 | 4,352.75 | 0.01733564 |
| `w2v-video-compiler` | 623 | 251.84 | 0.40423756 |
| `karma` | 8,363 | 192.99 | 0.02307692 |
| `hirag` | 2,225 | 136.92 | 0.06153846 |
| `kag-graph` | 10,956 | 126.42 | 0.01153846 |
| `kag` | 2,922 | 33.72 | 0.01153846 |
| `mlx-backend` | 130,820 | 11.99 | 0.00009167 |
| `worker-management` | 618 | 9.56 | 0.01546901 |
| `w2v-video-analyzer` | 957 | 7.35 | 0.00767783 |
| `w2v-asr` | 1,748 | 3.76 | 0.00215126 |
| `w2v-tts` | 3,520 | 1.75 | 0.00049624 |

⚠️ These are **COSTS, at zero margin.** They are not prices. A price is cost × markup,
and the markup is the operator's decision, not a measurement.

---

## 3. `tenant_config` — one row per tenant, and the default is already 30%

Columns: `tenant_id` (UNIQUE), `markup_pct numeric(6,2) NOT NULL`, `tenant_name`,
`billing_mode varchar(32) NOT NULL DEFAULT 'standard_markup'`, `notes`.

⚠️ **`billing_mode` currently has no value anywhere in the estate** — the column exists
only on `tenant_config` and `billing_invoices`, both empty, so every tenant rides the
code default. The four modes are `standard_markup`, `byo_platform_fee`, `pass_through`,
`internal`.

**Proposed, for the operator to change:** the only non-arbitrary starting point is to
state explicitly what is already implicit — `markup_pct = 30.00` (matching
`default_markup_pct`) and `billing_mode = 'internal'` for every tenant listed above,
since all of them ARE internal. A customer tenant then gets a deliberate row rather than
inheriting a default nobody chose.

⛔ Do not set `billing_mode = 'standard_markup'` on an internal cost centre just because
it is the column default. That is the difference between a cost allocation and a sale.

---

## 4. `tenant_access_charge` — the platform fee, and it is effective-dated

Columns: `tenant_id`, `service_key` (nullable = all services), `amount_usd`,
`cadence` (`monthly` | `one_time`), `included_cost_usd` (an allowance netted off usage),
`effective_from`, `effective_until`. UNIQUE partial index on
`(tenant_id, coalesce(service_key,''))` WHERE `effective_until IS NULL` — so **only one
open row per tenant/service**, and a change means closing the old row, not updating it.

**Proposed: no rows.** A platform fee is a commercial term with no measurable
counterpart — deriving one from spend would be inventing a number and dressing it as
analysis. This needs the operator's figure or nothing.

---

## 5. `markup_rules` — 4 rows, all expired, and that is the live gap

```sql
SELECT id, effective_from, effective_until FROM markup_rules ORDER BY id;
```

| id | effective_from | effective_until |
|---|---|---|
| 1 | 2026-06-19 10:02:56 | 2026-07-09 10:48:37 |
| 3 | 2026-08-10 12:31:36 | 2026-08-10 12:32:29 |
| 4 | 2026-08-10 12:34:59 | 2026-08-10 12:36:07 |
| 5 | 2026-08-10 20:17:34 | 2026-08-10 20:18:10 |

**0 effective.** Rows 3, 4 and 5 were open for 53 seconds, 68 seconds and 36 seconds
respectively — those are test artefacts, not a lapsed policy. Row 1 is the only one that
ever ran for a meaningful period.

So "all 4 markup rules expired" overstates it: **there has never been a sustained markup
rule.** Every tenant has always ridden `default_markup_pct` 30.0. The decision is
therefore not "renew the rules" but "is 30% the number, and does any tenant differ".

⚠️ `MarkupRule.markup_pct` is **nullable on purpose** — a rule may carry only an absolute
surcharge (`surcharge_*_per_1m_usd`, `surcharge_per_request_usd`). NULL and 0 are
declared equivalent there, pinned by `test_surcharge_only_rule_is_accepted`. ⛔ Do not
generalise the #167 NULL-vs-0 pricing contract to this column; that contract is about
rates, where an absent rate means *nobody said what this costs*.

---

## What the operator actually has to decide

| # | decision | can it be derived? |
|---|---|---|
| 1 | Is the markup 30%, and does any tenant differ? | **No** — commercial |
| 2 | Platform / access fee per tenant, and any included allowance | **No** — commercial |
| 3 | Per-request price for `workers` (cost is 0.01546901 for `worker-management`) | cost yes, price no |
| 4 | `billing_mode` per tenant — `internal` for all seven today | architecture says `internal`; confirm |
| 5 | Whether a fifth mode is wanted: BYO **and** a percentage | **No** — product |
| 6 | Seat fee | **blocked** — `seat_count=None` at `invoices.py:1058`, no validated seat source exists. Not shippable on an unvalidated source |

## What must happen before any of it is applied

1. **Fix emitter self-identification** so `unmapped:api` / `unmapped:autonomy` resolve to
   real keys. Another repo's change.
2. **Leave `INVOICE_CLOSE_ENABLED` unset** until 1 is done and the rates exist. It is
   absent from the running pod today, deliberately — gapless invoice numbering means a
   premature close cannot be undone.
3. **Expect the first close to REFUSE** on an unmapped key, and read that as the gate
   working.
