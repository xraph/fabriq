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

**Updated 2026-06-12 (grove commit a01144a):** the three grove gaps this
section originally worked around are now fixed upstream:

- `grove.Open` propagates its hook engine to `SetHooks`-capable drivers.
- `PgTx.txDB()` carries the hook engine, so hooks fire inside
  transactions, with the new `QueryContext.InTransaction` flag marking
  the path.
- `QueryContext.Conditions` is populated (best effort) for
  select/update/delete.

The backstop's policy with fixed grove:

- **Transaction path** (`InTransaction == true`): allow. fabriq stamped
  the tenant with `SET LOCAL` and RLS enforces isolation in the database
  — a stronger guarantee than any predicate inspection. The hook now
  *observes* this path (it used to be invisible), which keeps the trip
  counter honest.
- **Pool path**: deny with `ErrTenantHookTripped` + metric. In this
  architecture any pool-path access to a tenant table is a bug — denying
  outright remains stronger and simpler than predicate-sniffing, though
  `Conditions` now makes predicate assertions possible for hooks that
  want them (e.g. twinos privacy hooks).

Until grove cuts a release containing a01144a, fabriq's go.mod carries
`replace` directives to the local grove checkout (the same co-development
pattern twinos uses). Drop them on the next grove tag.

## The one RLS exception

`tag_readings` (Timescale hypertable) has no RLS — see ADR 0006. The
backstop's raw-SQL guard requires a literal `tenant_id` reference for
queries touching it.
