# Fabriq — the TWINOS data fabric

[![Go Version](https://img.shields.io/badge/go-1.25+-blue)](https://go.dev)

Fabriq is the only module in the TWINOS platform allowed to talk to a datastore.
It implements the invariants between stores:

- **Every write emits exactly one versioned event** — commands run in a Postgres
  transaction that appends to a transactional outbox; a leader-elected relay
  publishes to Redis Streams.
- **Every access is tenant-scoped** — tenant rides on `context.Context`, is
  stamped structurally into every engine (Postgres `SET LOCAL` + RLS, FalkorDB
  graph-per-tenant, ES index routing, Redis key prefixes), with a grove
  pre-query hook as a loud backstop.
- **Projections are always rebuildable from Postgres** — the knowledge graph
  and the search index are derived projections, never written directly.

## Architecture

```
        commands                queries (capability ports)        deltas
           │                            │                           ▲
           ▼                            ▼                           │
┌──────────────────────────────────────────────────────────────────┴─────┐
│ fabriq (facade)                                                        │
│  core/registry  core/command  core/event  core/projection  core/subscribe
│  ─────────────────────────── ports ──────────────────────────────────  │
│  adapters/postgres (grove)  adapters/redis  adapters/falkordb  adapters/elastic
└─────────────────────────────────────────────────────────────────────────┘
     Postgres+Timescale+pgvector   Redis Streams   FalkorDB    Elasticsearch
        (source of truth)           (fan-out)      (projection) (projection)
```

Binaries:

- `cmd/fabriq` — CLI (forge/cli): `migrate up|down|status`, `rebuild`, `reconcile`, `inspect`.
- `cmd/fabriq-worker` — outbox relay, projection consumers, reconciler (forge app).
- `cmd/api-example` — demo API: commands, queries, SSE fetch-then-subscribe (forge app).

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

// Reads: capability ports.
var a domain.Asset
err = f.Relational().Get(tenantCtx, "asset", res.AggID, &a)

// Live deltas: server-resolved channel, conflated, resumable.
deltas, err := f.Subscribe(tenantCtx, query.SubscribeScope{Entity: "asset", Scope: "site", ID: siteID})
```

Every call requires a tenant-stamped context (`tenant.WithTenant`), set
only by auth middleware from validated claims.

## Status

- **Implemented & integration-tested:** registry, command plane +
  transactional outbox, Postgres adapter (grove, RLS as non-superuser,
  Timescale bulk telemetry, pgvector), migrations + conformance test,
  Redis streams (relay fan-out, consumer groups, tailer), leader-elected
  outbox relay (LISTEN/NOTIFY wake), subscription hub (conflation, SSE,
  Last-Event-ID resume), `fabriq` CLI, `fabriq-worker`, `api-example`.
- **Scaffolded (phases 4–7):** FalkorDB execution (dialect translator is
  done + unit-tested; `adapters/graphtest` conformance suite ready),
  Elasticsearch adapter, projection engine/blue-green rebuild/reconciler,
  CRDT document plane (`core/document/DESIGN.md`).

## Development

```bash
make test              # unit tests (no Docker)
make test-integration  # testcontainers: PG+Timescale+pgvector, Redis
make bench             # benchmarks
make lint              # incl. depguard architecture boundaries
```

Decisions live in [docs/decisions](docs/decisions); runbooks in
[docs/OPERATIONS.md](docs/OPERATIONS.md); schema discipline in
[docs/MIGRATIONS.md](docs/MIGRATIONS.md).

Built on the Forge ecosystem: [grove](https://github.com/xraph/grove) (storage),
[forge](https://github.com/xraph/forge) (apps + CLI).

## License

Part of the Forge ecosystem.
