# Fabriq

**One write path. Every engine, in step.**

[![Go Version](https://img.shields.io/badge/go-1.25+-blue)](https://go.dev)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Fabriq is a standalone data fabric for Go. Every command commits once through a
transactional outbox, then fans out to relational, time-series, vector, graph,
and search engines — versioned, tenant-scoped, and always rebuildable.

It gives an application a single write path and typed read ports across multiple
storage engines while enforcing three structural invariants:

- **Every write emits exactly one versioned event** — commands run in a Postgres
  transaction that appends to a transactional outbox; a leader-elected relay
  publishes the event to Redis Streams.
- **Every access is tenant-scoped** — tenant rides on `context.Context` and is
  stamped structurally into every engine (Postgres `SET LOCAL` + row-level
  security, FalkorDB graph-per-tenant, Elasticsearch index routing, Redis key
  prefixes), with a pre-query hook as a loud backstop.
- **Projections are always rebuildable from Postgres** — the knowledge graph and
  the search index are derived projections, never written directly.

## Architecture

```
        commands                queries (capability ports)        deltas
           │                            │                           ▲
           ▼                            ▼                           │
┌──────────────────────────────────────────────────────────────────┴─────┐
│ fabriq (facade)                                                          │
│  core/registry  core/command  core/event  core/projection  core/subscribe
│  ─────────────────────────── ports ──────────────────────────────────  │
│  adapters/postgres  adapters/redis  adapters/falkordb  adapters/elastic  │
└──────────────────────────────────────────────────────────────────────────┘
     Postgres+Timescale+pgvector   Redis Streams   FalkorDB    Elasticsearch
        (source of truth)           (fan-out)      (projection) (projection)
```

Binaries:

- `cmd/fabriq` — the data fabric in one binary. `serve` runs the worker (outbox
  relay, projection consumers, reconciler, document plane); `migrate up|down|status`,
  `rebuild`, `reconcile`, and `inspect` are the operator commands. The default
  (no args) is `serve`.
- `cmd/api-example` — a demo API: commands, queries, and SSE fetch-then-subscribe.

## Quick start

```go
reg := registry.New()
_ = domain.RegisterAll(reg) // or your own entity pack

f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
    Postgres: fabriq.PostgresConfig{DSN: dsn},
    Redis:    fabriq.RedisConfig{Addr: redisAddr},
})

// Writes: the only path, one versioned event per command.
res, err := f.Exec(tenantCtx, command.Command{
    Entity: "asset", Op: command.OpCreate,
    Payload: &domain.Asset{Name: "Pump 7", SiteID: siteID},
})

// Reads: typed capability ports.
var a domain.Asset
err = f.Relational().Get(tenantCtx, "asset", res.AggID, &a)

// Live deltas: server-resolved channel, conflated and resumable.
deltas, err := f.Subscribe(tenantCtx, query.SubscribeScope{Entity: "asset", Scope: "site", ID: siteID})
```

Every call requires a tenant-stamped context (`tenant.WithTenant`), set only by
auth middleware from validated claims.

## Capabilities

All of the following are implemented and covered by integration tests:

- **Command plane & outbox** — registry-driven commands, optimistic concurrency,
  atomic batches, and a transactional outbox in Postgres.
- **Postgres source of truth** — row-level security verified as a non-superuser,
  Timescale hypertables for bulk telemetry, and pgvector (HNSW) for similarity
  search, with migrations and a registry-conformance test.
- **Redis Streams fan-out** — a leader-elected outbox relay (LISTEN/NOTIFY wake),
  consumer groups with `XAUTOCLAIM` recovery, and a subscription hub (delta
  conflation, SSE, Last-Event-ID resume).
- **Live queries** — maintained result sets: a `filter + sort + limit/cursor`
  subscription returns a snapshot, then exact `enter/leave/move/update` deltas as
  data changes (changefeed-style). The in-engine window stays an exact prefix of
  the Postgres-ordered result via a cushion + keyset boundary refill, so top-N is
  exact at all times. (P1: single-node maintained mode; sharding, a streamed mode,
  and a predicate index are on the roadmap.)
- **Graph projection (FalkorDB)** — an openCypher dialect behind a conformance
  suite (the engine-swap gate), a batched `TraverseAndHydrate`, and blue-green
  rebuilds verified to produce an identical graph.
- **Search projection (Elasticsearch)** — version-gated bulk writes,
  multi-field full-text search, lazy per-tenant index + alias provisioning, and
  atomic alias-swap rebuilds.
- **Reconciler** — per-aggregate drift detection (missing / stale / zombie)
  between Postgres and each projection, healed through the ordinary outbox rather
  than direct engine writes.
- **CRDT document plane** — an append-only update log folded through a merge
  engine, with sequence-vector sync, compaction, and quiet-window materialization
  that emits a single ordinary versioned event, so collaborative documents are
  normal entities downstream.
- **Observability** — a W3C `traceparent` stamped on every event envelope by
  default, plus Prometheus metrics exposed by the worker (`fabriq serve`,
  `/metrics`).

## Development

```bash
make test              # unit tests (no Docker)
make test-integration  # testcontainers: PG+Timescale+pgvector, Redis, FalkorDB, Elasticsearch
make bench             # benchmarks
make lint              # incl. depguard architecture boundaries
```

Operational runbooks live in [docs/OPERATIONS.md](docs/OPERATIONS.md), and
schema discipline in [docs/MIGRATIONS.md](docs/MIGRATIONS.md).

Fabriq builds on [grove](https://github.com/xraph/grove) for storage and
[forge](https://github.com/xraph/forge) for application and CLI scaffolding.

## License

Licensed under the Apache License, Version 2.0. You may obtain a copy of the
License in the [LICENSE](LICENSE) file or at
<http://www.apache.org/licenses/LICENSE-2.0>.

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied.

Copyright 2026 xraph.
