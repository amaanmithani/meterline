# meterline — spec

Usage metering and billing for API products: ingest usage events at high rate,
count each one exactly once even when producers and consumers retry, roll them up
per customer, price them against a plan, preview invoices, and report usage to
Stripe. ModelMux's usage events are a first-class input.

## Goals (v1)

| # | Capability | Done when |
|---|---|---|
| G1 | Ingest API `POST /v1/events` (batches, JSON): validates, assigns nothing, produces to Redpanda keyed by customer | accepts ModelMux usage events (schema v1) and a generic `{id, customer, meter, value, ts}` form |
| G2 | **Exactly-once counting.** Duplicate events (same `id`) from retrying producers, and re-delivered batches from a crashed consumer, never change totals | a test replays the same events with duplicates and a consumer crash mid-batch, and invoice totals are identical |
| G3 | Storage in ClickHouse: raw events (deduplicated by id) + hourly rollups for dashboards | queries by customer / meter / hour return in < 100 ms at 10M events |
| G4 | **Late events.** A billing period closes after a grace window; events that arrive later are booked as adjustments to the next invoice, not silently dropped or retro-edited | test with events arriving after close |
| G5 | Plans and pricing: per-meter unit price, included quota, graduated tiers; invoice preview `GET /v1/customers/{id}/invoice?period=YYYY-MM` | pricing unit tests incl. tier boundaries, exact decimal math (no floats for money) |
| G6 | Stripe sync: report per-customer meter totals to Stripe Billing meter events with idempotency keys (against `stripe-mock` in tests) | re-running sync doesn't double-report |
| G7 | Observability: Prometheus metrics (ingest rate, consumer lag, dedup hits, late events), `/healthz` | |
| G8 | Read-only dashboard page (usage by customer/meter/hour) | hosted demo fed by ModelMux traffic |

## Non-goals (v1)

Multi-region, proration of plan changes mid-period, taxes, invoice PDFs, Kafka
schema registry.

## Architecture

```
producers ──POST /v1/events──► ingest ──produce (key = customer)──► Redpanda topic "usage"
                                                                         │
                                          consumer group ◄───────────────┘
                              batch by partition, insert with a dedup token = partition:first-last offset
                                                   │
                                                   ▼
                               ClickHouse  events  (ReplacingMergeTree, ORDER BY customer, meter, id)
                                           hourly  (materialized rollup for dashboards)
                                                   │
                         pricing ◄── invoice preview API ──► Stripe meter events (idempotency key)
```

Two independent layers make counting exactly-once:

1. **Consumer retries.** Each ClickHouse insert carries a deterministic dedup token
   derived from the Kafka offsets it covers. A consumer that crashes after inserting
   but before committing offsets re-inserts the same batch with the same token, and
   ClickHouse drops it.
2. **Producer duplicates.** Events with the same `id` collapse in a
   `ReplacingMergeTree`. Invoices read with `FINAL` (exact). Dashboard rollups
   are approximate until merges settle, and the dashboard says so.

## Success metrics (measured, committed)

- Sustained ingest events/s end to end (HTTP → Redpanda → ClickHouse) and p50/p99
  lag from event acceptance to queryable, on one machine, with the machine spec.
- Exactly-once proof: totals identical across runs with 0%, 10% and 50% duplicate
  events and injected consumer crashes.
- Coverage ≥ 85% on Go packages; integration tests against real Redpanda and
  ClickHouse containers in CI.

## Stack

Go 1.25, franz-go (Kafka protocol), clickhouse-go v2, stripe-go against stripe-mock,
Prometheus client. Docker Compose for Redpanda, ClickHouse and stripe-mock.
