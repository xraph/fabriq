# ADR 0002 — Tenancy enforcement layers (and where the grove hook actually sits)

**Status:** accepted · 2026-06-12

## Layers, strongest first

1. **Stamped transactions + RLS (primary).** Every tenant-table operation
   — reads included — runs inside a transaction the adapter stamps with
   `SELECT set_config('app.tenant_id', $1, true)`. RLS policies (FORCE)
   key on that setting, so even arbitrary SQL through the raw escape hatch
   cannot cross tenants. Unstamped sessions see zero rows. Applications
   MUST connect as a non-superuser role (RLS never constrains superusers);
   the test harness provisions `fabriq_app` accordingly.
2. **Structural stamping.** The command executor forces `id`, `tenant_id`,
   `version` from context — payload values are ignored or, for a foreign
   tenant_id, rejected. Channel names, graph names, index names and cache
   keys are derived from the context tenant in exactly one place
   (`core/registry/derive.go`).
3. **Grove hook backstop.** A pre-query/pre-mutation hook DENIES any
   pool-path grove query against a tenant table with
   `ErrTenantHookTripped` and a metric trip.

## Why the backstop denies instead of asserting predicates

Two grove facts shape this (verified against grove v1.5.2 source):

- `grove.Open` does **not** attach its hook engine to the driver; the
  adapter calls `PgDB.SetHooks` explicitly.
- `PgTx.txDB()` builds the per-query `PgDB` **without the hooks field**,
  so in-transaction queries bypass hooks entirely.

Since fabriq's own paths all run through stamped transactions (where RLS
is the enforcement), the only surface grove hooks can see is the pool
path — and in this architecture *any* pool-path access to a tenant table
is a bug. Denying outright is both stronger and simpler than predicate
inspection (which the hook context couldn't support anyway: pgdriver does
not populate `Conditions` for selects).

Suggested upstream grove improvements (not required by fabriq):
propagate hooks into `txDB()`, populate `QueryContext.Conditions`.

## The one RLS exception

`tag_readings` (Timescale hypertable) has no RLS — see ADR 0006. The
backstop's raw-SQL guard requires a literal `tenant_id` reference for
queries touching it.
