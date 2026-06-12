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

## Development

```bash
make test              # unit tests (no Docker)
make test-integration  # testcontainers: PG+Timescale, Redis, FalkorDB, ES
make bench             # benchmarks
make lint              # incl. depguard architecture boundaries
```

Built on the Forge ecosystem: [grove](https://github.com/xraph/grove) (storage),
[forge](https://github.com/xraph/forge) (apps + CLI).

## License

Part of the Forge ecosystem.
