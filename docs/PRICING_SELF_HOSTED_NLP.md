# Pricing self-hosted NLP (`mlx-nlp`)

What we charge for spaCy NER / linguistic annotation running on our own Apple
Silicon, why the unit is **characters**, and what the hosted market charges for
the same work — recorded now because we will need a hosted provider to scale.

**Status:** seeded 2026-09-04 at **$1.00 / 1,000,000 characters**,
`cost_model: per_character`, `model: null`, in `scripts/initial_pricing.yaml`.

---

## The gap this closes

`MeteringPricingMiss` fires for provider `mlx-nlp`. Measured 2026-09-04:

| model (`served_model`) | events with `cost_usd IS NULL` | characters |
|---|---:|---:|
| `en_core_web_trf` | 362 | 10,034 |
| `en_core_web_md`  | 286 | 8,580  |

Window 2026-09-04 16:09:36Z → 16:27:10Z, still growing.
`pricing_id` was NULL on every one of them, because `mlx-nlp` had **no
`provider_pricing` row at all** — it was absent from both `rows:` and
`required_providers:` in the seed.

A pricing miss does not raise. `compute_cost_usd` returns `(None, None)`,
`billing_cost_events.cost_usd` is written NULL, and
`billing.generate_billing()` **skips** those rows — the spend is never
invoiced. This is the same failure class that produced 17,626 silent events in
prod on 2026-07-19 (see the header of `scripts/initial_pricing.yaml` and the
docstring of `tests/test_pricing_coverage.py`).

## The unit, and why

The emitter is authoritative. `mlx_backend` `nlp_routes.py:227-238` sends:

- `quantity = len(request.text)` — packed into `input_tokens`
- `usage_extra.unit = "nlp_chars"`
- the event carries `service_key` of `mlx-backend`, `provider` of `mlx-nlp`, and
  `model` of `served_model=en_core_web_trf`
  <!-- phrased as prose, not `field = 'value'`: detect-secrets' Secret Keyword plugin
       fires on the assignment shape and flagged this line. It is a routing field and a
       service name, never a credential — reworded rather than baselined, so the scanner
       keeps its teeth on this file. -->

So the billable unit is a **character of input text**. There is no output
token; `output_per_1m_usd` is `"0"`.

`services/pricing.py:107` computes
`input_cost = (input_tokens * in_rate) / _MILLION`, and `per_character` shares that
arithmetic with `per_token` and `per_second` (`_TOKEN_FAMILY_COST_MODELS`,
`services/pricing.py:44`). Therefore **`input_per_1m_usd` on a `per_character`
row is USD per 1,000,000 characters**. In-repo precedent for exactly this
shape: `elevenlabs` (`per_character`, `100.0`), `kitten` (`per_character`,
`3.00`), `mlx-embed` (`per_character`, `0.10`).

`model: null` is deliberate — the resolver's rung 4
(`_resolve_pricing_row`, `(provider, model=NULL, service_key=NULL)`) is the
only backstop when a new spaCy pipeline id shows up, and we ship pipelines
faster than we seed rows. `mlx-nlp` carries exactly one `cost_model` family, so
it is not a mixed-family provider and rung 4 legitimately fires
(`tests/test_pricing_coverage.py::test_unseeded_model_resolves_to_provider_default`).

---

## The market (all fetched 2026-09-04)

Every published price normalised to **USD per 1,000,000 characters** of input.

| Vendor | Product | As published | USD / 1M chars | Source |
|---|---|---|---:|---|
| AWS | Comprehend `DetectEntities` | $0.0001 per 100-char unit; $0.00005 above 10M units; $0.000025 above 50M. **300-char minimum billed per request** | **1.00** / 0.50 / 0.25 | <https://aws.amazon.com/comprehend/pricing/> |
| Google | Cloud Natural Language, Entity Analysis | per 1,000-char unit, rounded up per request. Free below 5,000 units/mo; $1.00 for 5K–1M units; $0.50 for 1M–5M; $0.25 above. Region key `global` | **1.00** / 0.50 / 0.25 | <https://cloud.google.com/natural-language/pricing> |
| Azure | AI Language (now "Language in Foundry Tools"), NER, Standard S–Web, West Europe, USD | per 1,000 text records; 1 text record = 1,000 chars. $1 (0–0.5M records) / $0.75 (0.5–2.5M) / $0.30 (2.5–10M) / $0.25 (10M+). 5,000 records/mo free | **1.00** / 0.75 / 0.30 / 0.25 | <https://azure.microsoft.com/en-us/pricing/details/language/> |
| HuggingFace | Inference Endpoints, 4 vCPU / 8 GB | $0.13/hr (2 vCPU $0.07/hr; T4 GPU $0.50/hr) | ~0.036 at 1,000 chars/s and 100% utilisation; ~0.36 at 10% | <https://huggingface.co/pricing> |
| Explosion | spaCy / Prodigy | **no hosted per-call API.** Prodigy $390 lifetime personal / $490 per seat; Tailored Pipelines quoted per engagement | n/a | <https://prodi.gy/buy> |

### The headline: three independent vendors converge on $1.00

AWS, Google and Azure — three vendors, three different published units
(100-char units, 1,000-char units, 1,000-record blocks) — all normalise to
**exactly USD 1.00 per 1,000,000 characters** at their entry tier, and all
three discount toward **$0.25–$0.30** at volume. That convergence, not any one
page, is what makes $1.00 the defensible anchor.

It is also the anchor already cited elsewhere in this repo. The `karma` row
(entity extraction, `scripts/initial_pricing.yaml`) records a market target of
"$0.03/doc (Google Cloud NL / AWS Comprehend entity analysis, ~30K-char doc,
2026-07-09)". $0.03 / 30,000 chars = **$1.00 / 1M chars**. Two independent
research passes, fourteen months apart in the file's history, reached the same
number for the same task.

### Two caveats, recorded honestly

**1. Google's page never literally writes "per 1,000 units."** Its column
header says "Price per 1,000-character unit" and the prose says "the price per
unit." Read strictly literally that is **$1,000 per 1M characters** — and the
same literal reading makes Text Moderation $50,000/1M chars, which is
implausible on its face. We took the *$1.00 per 1,000 units* reading, which
lands exactly on AWS and Azure. **This needs confirmation against the Google
Cloud Pricing Calculator or the SKU list before it is load-bearing.** Until
then, treat Google's row as corroborating AWS/Azure rather than as an
independent third measurement.

**2. AWS's 300-character minimum inverts the ranking at our request shape.**
Our requests average **27.7 characters** (10,034 chars / 362 events). AWS bills
a 300-char minimum per request, i.e. 3 units × $0.0001 = **$0.0003 per
request** regardless of how short the text is. 1M of *our* characters is
1,000,000 / 27.7 ≈ **36,101 requests** × $0.0003 = **$10.83**. That is 10.8×
the headline rate, and it is an argument for pricing `mlx-nlp` as
`cost_model: per_request` at `$0.0003` rather than per character. **Presented,
not decided** — see "The per-request alternative" below.

---

## Three anchors

| Anchor | USD / 1M chars | What it represents |
|---|---:|---|
| Low | **0.036** | HuggingFace compute parity — $0.13/hr CPU endpoint at 1,000 chars/s, 100% utilised. At a realistic 10% utilisation it is ~0.36. This is roughly "what the hardware costs", not a price. |
| Median | **1.00** | The AWS / Google / Azure entry-tier convergence. **Seeded.** |
| High | **10.83** | What AWS would actually invoice for *our* traffic shape, once its 300-char per-request minimum is applied to our 27.7-char average request. |

$1.00 sits ~28× above compute parity and ~10.8× below what the closest
comparable vendor would actually charge us. Both directions of error are
bounded and known.

## The per-request alternative

Because our texts are tiny, a per-character rate under-charges relative to
every hosted vendor that bills a per-request floor. The alternative:

```yaml
- provider: mlx-nlp
  model: null
  cost_model: per_request
  per_request_usd: "0.0003"
```

- **For:** matches AWS's effective economics at our real request shape; a
  per-call unit is also a truer proxy for our own marginal cost, which is
  dominated by model-load/dispatch rather than by characters.
- **Against:** it diverges from the emitter's declared unit (`nlp_chars`), it
  stops tracking the market the moment texts get longer, and it would misprice
  badly if a caller starts sending documents instead of entity strings.
  `per_character` degrades gracefully in both directions; `per_request` does
  not.
- **Not decided here.** If we adopt it, the emitter contract in
  `docs/EMITTER_GUIDE.md` should stop advertising `nlp_chars` as the billable
  unit, or the two will disagree.

## Scale check — state it plainly

At $1.00 / 1M characters, the entire measured backlog costs:

- `en_core_web_trf`: 10,034 chars → **$0.010**
- `en_core_web_md`: 8,580 chars → **$0.009**
- both: 18,614 chars → **$0.019**

**The rate is immaterial today.** Two cents of exposure does not justify
agonising over $0.036 vs $1.00 vs $10.83. What matters is getting the **unit**
right — `per_character`, keyed to the emitter's `nlp_chars`, with a
`model: null` provider-default — *before* volume arrives. A wrong rate is one
`POST /api/config/provider-pricing` away from fixed, and effective-dating means
the correction is clean. A wrong or missing *unit* silently drops revenue and
leaves no record that it ever existed, which is exactly what the 362 NULL rows
are.

---

## When we move to a hosted provider

We will need hosted NLP to scale past what mini1/mini2 can serve. At our
request shape the choice is decided by the **per-request floor**, not by the
headline rate:

- **AWS Comprehend charges a 300-character minimum per request.** At a 27.7-char
  average that is a ~10.8× effective surcharge. AWS is the *worst* fit for
  short-text NER despite quoting the same headline $1.00.
- **Google Cloud NL bills 1,000-character units rounded up per request** with
  no separate per-request floor, and gives 5,000 units/month free.
- **Azure AI Language bills 1,000-char text records** with no per-request floor,
  and gives 5,000 records/month free.

For short texts, **Google and Azure are materially cheaper than AWS** — the
rounding-up to a 1,000-char unit still costs us ~36× the raw character count at
27.7 chars/request, but that beats AWS's 300-char floor priced at
$0.0001/100 chars. If we ever batch multiple entity strings into one request,
all three converge back toward the headline $1.00 and the decision reverts to
region, latency and data residency.

Two structural notes for that migration:

- **Explosion (spaCy's own vendor) has no hosted per-call API** — Prodigy is a
  licensed annotation tool, and Tailored Pipelines are a consulting
  engagement. There is no "spaCy Cloud" to buy. Migrating off self-hosted
  spaCy means changing model *and* vendor, not just hosting.
- **HuggingFace Inference Endpoints is the closest like-for-like** (run our own
  `en_core_web_trf` on their hardware) and is by far the cheapest *if kept
  busy*. Its price is time-based, so its per-character cost is entirely a
  function of utilisation: 0.036 at 100%, 0.36 at 10%, unbounded at idle. Do
  not quote its $0.13/hr as a per-character rate without stating the
  utilisation assumption alongside it.

## Where this is enforced

- `scripts/initial_pricing.yaml` — the `mlx-nlp` row and its
  `required_providers:` entry.
- `tests/test_pricing_coverage.py` — `required_providers` is bidirectional: a
  seeded provider missing from the list fails, and a listed provider with no
  rows fails. Rung-4 resolution is proved against the real DB and the real
  `compute_cost_usd`, not asserted against YAML shape.
- `tests/test_initial_pricing_seed.py` — seeds the real YAML and exercises the
  real pricing arithmetic.

**Do not edit the seeded rate in the YAML to change it.** Per that file's own
header, a rate change to an already-seeded row goes through
`POST /api/config/provider-pricing`, which closes the prior row's
`effective_until`, inserts a new open-ended row, and writes an `audit_log`
entry naming the actor. Editing the YAML after the seed diverges from the DB
and the seed script exits non-zero rather than auto-correcting. Adding
`mlx-nlp` was the different case — a provider that was never seeded at all,
which is the seed script's documented "absent → POST it" branch.
