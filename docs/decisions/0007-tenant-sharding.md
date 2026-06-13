# ADR 0007 — Tenant sharding of the source of truth (routing, not consensus)

**Status:** proposed · 2026-06-13

## Context

fabriq scales out today: the request path is stateless (every `Exec` is one
Postgres transaction, N replicas run concurrently), projection consumers
scale by replica via Redis consumer groups, and the singleton background
runners (outbox relay, reconciler, document plane) are leader-elected with
`pg_try_advisory_lock` (ADR 0004). What it does **not** do is distribute its
*source of truth*: Postgres is a single linearizable anchor, and everything
else is a derived, rebuildable read model.

That anchor has one ceiling — single-Postgres write throughput, and behind
it the single global relay leader. Every other tier already scales
horizontally. So the only "make fabriq more distributed" worth doing is a
horizontal-write path for the source of truth. This ADR records *how*, and
deliberately bounds the ambition: fabriq will **not** grow multi-master
writes, its own consensus, distributed transactions, or a distributed query
engine. Those would trade away the simplicity the three invariants buy and
re-implement (worse) what Postgres already gives us.

## Decision

Shard the source of truth **by tenant**. A tenant's entire aggregate
history, event log, and outbox live on exactly one shard. Sharding enters
the codebase as **a new routing adapter behind the existing ports** plus a
per-shard loop in the worker — not as a change to `core/`, the `Fabric`
facade, the command executor, or any call site.

The single-DSN configuration remains valid as the degenerate one-shard case,
so existing deployments are unaffected.

## Why this is a routing problem, not a consensus problem

Two existing invariants collapse the hard parts of distribution:

1. **Tenant scoping is structural** (ADR 0002). Every port operation carries
   a tenant on `ctx` (`tenant.Require`), and the port API has *no*
   cross-tenant query. So there is no scatter-gather read to build: a query
   routes to one shard, full stop.
2. **A command never spans tenants.** `Exec` is single-tenant; `ExecBatch`
   is one tenant's commands in one transaction. So the write path is a route
   to one shard's local transaction — **no two-phase commit, no sagas**. The
   aggregate row, its event, and the outbox row still land atomically in one
   local Postgres transaction, exactly as today.

Sharding is therefore a directory lookup plus per-shard background runners.
No new distributed-systems machinery.

## Design

### The seam: one routing port

`Open` hands a single Postgres adapter in as `Ports.Store` / `Relational` /
`Vector` / `Timeseries`. The change makes those route by `ctx` tenant. The
interfaces do not move — `command.Store` and `query.RelationalQuerier` gain
a sharded implementation in a new `adapters/shard` package:

```go
type Relational struct{ set *Set }

func (r *Relational) Get(ctx context.Context, entity, id string, into any) error {
    pg, err := r.set.For(ctx)      // tenant.Require -> directory -> shard adapter
    if err != nil { return err }
    return pg.Get(ctx, entity, id, into)   // one-line delegate; same for GetMany/List/Query
}
```

Routing happens *at* the store boundary, so the whole write transaction opens
on the routed shard. The executor, talking only to `command.Store`, is
unchanged; the facade and `repo.Get/List/SearchWith/...` never learn shards
exist. This is the hexagonal payoff — sharding is one more adapter behind a
port (and `core/` keeps its ADR 0001 extractability: zero shard knowledge).

### Tenant → shard directory

```go
type Set struct {
    shards map[string]*postgres.Adapter   // shardID -> adapter (own DSN/pool)
    dir    Directory                      // tenantID -> shardID, version
}
func (s *Set) For(ctx context.Context) (*postgres.Adapter, error) {
    tid, err := tenant.Require(ctx); if err != nil { return nil, err }
    id, err := s.dir.Shard(ctx, tid)
    if err != nil { return nil, err }
    return s.shards[id], nil
}
```

The directory is an **authoritative table** (`tenant_id → shard_id, version`)
in a small catalog database, not pure hashing — it supports uneven tenant
sizes and *controlled* tenant moves. It is cached in-process with TTL +
version invalidation, the same idiom already used for projection-state
resolution (`cachedState`, `liveGraphResolver` in `open.go`). Consistent
hashing/rendezvous is the fallback only if tenant count grows past a directory.

### Config + Open

```go
type ShardConfig struct { ID, DSN string; PoolSize int }
// Config gains: Shards []ShardConfig, Catalog PostgresConfig.
// The existing single Postgres field stays as 1-shard sugar.
```

`Open` dials each shard into `map[id]*postgres.Adapter`, builds the `*Set`,
and points the routing ports at it. `Stores` grows `Shards *shard.Set`.

### Per-shard relay and reconcile leadership — free

The elector holds `pg_try_advisory_lock` on a dedicated connection of *one*
adapter (ADR 0004), and advisory locks are scoped per database. Running the
elector against each shard yields **one relay leader per shard
automatically** — no new election mechanism:

```go
for _, sh := range stores.Shards.All() {
    elector := postgres.NewElector(sh.PG, lockKeyRelay) // 1001; distinct DB = independent
    go elector.Run(ctx, func(lead context.Context) error { return runRelay(lead, sh.PG) })
}
```

This also dissolves the single-global-relay bottleneck: relay throughput now
scales linearly with shard count. The reconciler fans out the same way (key
1002 per shard), scanning its own shard's tenants — isolated failure domains.

Caveat: if two shards co-reside in one Postgres *database*, derive per-shard
keys (`lockKeyRelay*1000 + idx`) so leaders stay independent. Shards on
distinct servers — the normal case — need no change. Lock keys otherwise stay
as ADR 0004 fixed them.

### Projections are almost untouched

The relay still publishes to the shared Redis stream (`registry.StreamKey()`);
events still carry `tenant_id`; consumer groups still scale by replica with no
election. The only place shards surface is hydration-from-Postgres
(rebuild snapshot, reconcile truth, `TraverseAndHydrate`), which already takes
a tenant-stamped `ctx` and so routes transparently once the rebuilder /
reconciler `Snapshot` / `Truth` / `Repair` are the routing store. Falkor and
Elasticsearch targets are keyed by tenant *name*
(`GraphName`, `SearchIndexAlias`), orthogonal to PG shard — so projection-store
topology and PG sharding evolve independently.

### Placement reconciler

The directory introduces one genuinely cross-shard concern: **placement
drift** — a tenant whose directory entry points at shard A but whose rows are
(partly) on B after an interrupted move, or orphaned data. A separate,
read-only **placement reconciler** audits the directory against each shard's
distinct `tenant_id` set and flags mismatches/orphans. It is rare and global,
so it runs under one global leader, distinct from the per-shard drift
reconcilers.

### Migrations and tenant moves

`fabriq migrate up` becomes a loop: each shard is a full schema, advisory-locked
independently, plus the catalog migration. Deploy gate: all shards at version
N before app rollout. The Helm migrate-Job loops shards (or one Job per shard).

Moving a tenant (rebalance) is the only hard operation, and it is offline-ish,
not hot-path:

1. directory → `migrating` (brief per-tenant write freeze),
2. copy that tenant's rows + event log + outbox A→B (RLS makes the
   tenant-scoped selection clean),
3. flip directory → B, bump version (cache invalidation),
4. **rebuild projections on the far side rather than copy them** — because
   projections are rebuildable from Postgres (invariant #3), the irreducible
   thing to move is just rows + log,
5. reconcile, then drop from A.

The third invariant is what makes resharding tractable.

## What this does NOT change

- `core/*` — zero shard knowledge; ADR 0001 extractability holds.
- The `Fabric` facade interface, `command.Executor`, the subscription hub.
- The port interfaces (`Store`, `Relational`, `Vector`, `TSQuerier`).
- Every call site (`f.Exec`, `repo.Get`, `repo.SearchWith`, …): `ctx` already
  carries the tenant.
- The elector mechanism and advisory-lock keys (ADR 0004).
- Redis / FalkorDB / Elasticsearch topology (shard those separately, later,
  only if they become the bottleneck).

## Consequences

- **Write throughput scales linearly with shard count**, and so does the
  relay. The single-Postgres / single-relay ceiling is removed along the one
  axis that had it.
- **Blast radius shrinks.** A downed shard makes *its* tenants unavailable for
  writes and truth reads; every other tenant is unaffected. Sharding is a
  resilience win, not just a throughput one.
- **No distributed transaction anywhere.** One tenant → one shard → one local
  ACID transaction (row + event + outbox).
- **`ExecBatch` must reject mixed-tenant batches.** Today a scoping nicety;
  under sharding it is a routing-correctness invariant — make it explicit.
- **Cost:** a directory to operate (placement, caching, invalidation), a
  per-shard migrate/deploy discipline, and tenant-move tooling. These are the
  price of the write-path ceiling going away; they are paid in operations, not
  in the programming model.
- **Per-shard guarded tables** (e.g. the `tag_readings` Timescale exception,
  ADR 0006) replicate per shard; nothing about that exception changes.

## Sequencing

1. `adapters/shard` routing ports with the single-shard degenerate case +
   tests — zero behaviour change, pure refactor.
2. Multi-shard `Open` + directory + per-shard relay/reconcile elector loops.
3. Per-shard migrate loop + placement reconciler.
4. Tenant-move tooling last (it leans on everything above).

Step 1 is real and risk-free today; steps 2–4 land when a single Postgres
actually runs out. Until then the one-shard path is the production path.
