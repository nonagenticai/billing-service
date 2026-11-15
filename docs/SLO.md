# metering_service Service-Level Objectives

> Source-of-truth for what "healthy" means. Alerts in
> `gitops-production/clusters/production/services/alerting/rules/prometheusrule-metering.yaml`
> are calibrated to wake an oncall when these SLOs are at risk of breaching.

## Why these SLOs

metering is the billing path: every request gated by `license-authz`
consults `/api/quota/check` and `/api/budget/check`, every LLM call
emits a `cost_event` that has to be priced and stored, and every
admin-driven config change has to be auditable. The four SLOs below
cover the four ways revenue actually leaks:

1. **Lost billable events** — a flush that fails drops a batch of
   `metering_events` / `cost_events` we can never re-create.
2. **Mispriced cost events** — a `cost_usd = NULL` row is invisible to
   `billing.generate_billing()`, so we under-bill.
3. **Slow quota/budget reads** — license-authz fans these out on the
   request hot path; a p95 spike cascades into ingress latency for
   every gated request in the cluster.
4. **Stale exec_quality** — the continuous-improvement signal feeds
   workflow + prompt evals; if it's hours behind, the improvement loop
   is making decisions on yesterday's data.

## SLO table

| # | SLI                                                | Target  | Measurement window | Burn-rate alert                                           |
|---|----------------------------------------------------|---------|--------------------|-----------------------------------------------------------|
| 1 | Event + cost-event flush success ratio             | ≥ 99.9% | 30d rolling        | `MeteringFlushFailureBurnRate` — >1% errors over 10m      |
| 2 | Cost-event pricing coverage (no `pricing_miss`)    | ≥ 99.5% | 7d rolling         | `MeteringPricingMissSpike` — >0.1/s per provider/model 15m|
| 3 | `/api/quota/check` + `/api/budget/check` p95       | < 500ms | 30d rolling        | `MeteringQuotaBudgetLatencyHigh` — p95 >500ms over 5m     |
| 4 | exec_quality ETL freshness (watermark lag)         | < 5min  | 7d rolling         | `ImprovementLoopExecQualityEtlLag` — lag >10m for 10m ⚠️ owned by `improvement-loop-service`, not this service |

Plus two infrastructure-level guards:

| # | Alert                                | What it catches                                                                 |
|---|--------------------------------------|---------------------------------------------------------------------------------|
| 5 | `MeteringDown`                       | 0 Prometheus scrape targets up for 2m                                          |
| 6 | `MeteringQuotaRedisFallbackHigh`     | >20% of quota checks bypassed Redis to DB for 10m (Redis cold/under-provisioned)|

## Error budgets

| SLI                            | Budget per 30d                                       |
|--------------------------------|------------------------------------------------------|
| Flush success                  | 0.1% of flushed events ≈ 43 min "all flushes failing"|
| Pricing coverage (per model)   | 0.5% of cost events for that provider/model         |
| Quota/budget p95               | n/a — latency SLO, not availability                  |
| ETL lag                        | n/a — freshness SLO; budget is "minutes behind"      |

When >50% of a budget is consumed in 7 days, treat the corresponding
follow-up work as a P1 (e.g. add the missing `ProviderPricing` row,
scale Redis, fix the flusher's slow path).

## What the SLOs deliberately do **not** cover

- **Tracing completeness.** OTLP traces export to Alloy on a best-effort
  basis; missing traces are an observability paper-cut, not a billing
  failure.
- **Audit log lag.** The audit insert is part of the same transaction as
  the underlying mutation; lag is bounded by transaction commit, not by
  a separate pipeline.
- **Dashboard rendering.** The Jinja2 `/dashboard/*` HTML is operator
  convenience; downtime there does not affect the billing path.

If any of those are persistently broken, it's a real bug — file an
issue, but don't page oncall.
