# Migrations

The `migrations/` package is fabriq's **DDL authority** — grove Go-code
migrations in group `fabriq`. The registry never generates DDL; the
registry-conformance integration test (migrations package) diffs
`information_schema` against every registered spec in both directions and
fails CI on drift.

## Running

```bash
fabriq migrate up      # apply pending (grove advisory migration lock held)
fabriq migrate status  # applied/pending per group
fabriq migrate down    # roll back the most recent migration
```

Migrations are a **discrete deploy step**, never run at app startup.
`fabriq migrate` connects as the schema owner; applications connect as a
non-superuser role (RLS does not constrain superusers — the integration
harness provisions `fabriq_app` exactly this way).

## Expand / contract discipline

Every schema change ships in two (or three) releases:

1. **Expand** — add the new column/table/index, nullable or defaulted.
   Old and new code both run.
2. **Migrate** — deploy code reading/writing the new shape; backfill in
   batches if needed (a migration may do this, keeping batches < 5k rows).
3. **Contract** — once nothing reads the old shape, drop it in a later
   migration.

Never: rename in place, change a column type in place, add NOT NULL
without a default in one step.

## What lives in this stream

Everything Postgres-side, in one ordered group: tables, indexes, **RLS
policies** (0004), **Timescale** hypertable + compression (0005),
**pgvector** extension/table/HNSW index (0006), CRDT plane tables (0007).
Grove migration executors run statements outside explicit transactions
(autocommit), so `CREATE INDEX CONCURRENTLY` is usable in this stream when
a large-table index lands.

Notes:

- `tag_readings` deliberately has **no RLS**: TimescaleDB columnstore
  refuses tables with row security. Tenancy there is structural (the
  TSQuerier stamps `tenant_id` into every statement) plus the adapter's
  raw-SQL guard. See `docs/decisions/0006-timescale-rls.md`.
- `fabriq_outbox`, `fabriq_projection_*` have no RLS by design: they are
  worker-plane tables read across tenants by the relay/consumers and are
  unreachable through the application ports.
- Timescale/pgvector migrations skip quietly when the extension is
  unavailable, so plain-Postgres dev environments still migrate.

## Embedding in a host app (twinos)

Host applications compose groups: depend on `migrations.GroupName`
("fabriq") via `migrate.DependsOn` so host migrations order after
fabriq's, and run both through one orchestrator.
