# ADR 0014 — Per-tenant customer-facing analytics

**Status:** accepted · 2026-07-11

## Context

ADR 0013 built a cross-tenant analytics **sink**: a deliberate, narrow,
operator-only exception to fabriq's no-cross-tenant-queries invariant, used
for fleet-wide reporting. That ADR explicitly listed "tenant-facing analytics
surfaces" as future work, out of scope, not just unbuilt.

Tenants still need their own analytics: a SaaS built on fabriq wants its own
users to see usage dashboards, funnels, and aggregations over both custom
events and the tenant's own domain data — without operators in the loop and
without any of that data leaving the tenant's isolation boundary. The
question this ADR answers: can fabriq offer a first-class, per-tenant
analytics capability that a tenant's own application can query directly,
while keeping every invariant the rest of fabriq relies on (RLS, structural
tenant stamping, the three-gate backstop) fully intact?

The design was worked out in
`docs/superpowers/specs/2026-07-10-per-tenant-analytics-design.md`
(gitignored-local); this ADR records what was actually decided and built.

## Decision

Yes — a new facade port, `f.Analytics()`, backed by a store that lives
**inside the tenant's own database**, built from these decisions:

- **D1 — In-tenant store, no separate DSN.** Unlike ADR 0013's sink (a
  separate co-located database, DSN-validated distinct from every tenant
  DSN), the tenant-analytics store is just more tables in the tenant's own
  database/schema/shard: `fabriq_insights_events` (append-only customer
  events) and `fabriq_insights_facts` (latest projected state of opt-in
  domain entities), added by migration `202607100031`. There is nothing to
  validate at boot because there is no second connection — `Config.Insights`
  is a plain on/off switch (`InsightsConfig.Enabled`), not a DSN.
- **D2 — RLS inherited, not reimplemented.** Both tables carry `tenant_id`
  and `scope_id` and get the same `ScopeAwareTenantPolicy` RLS policies as
  every other in-tenant table (migration `0031_insights.go`). `Track`,
  `Query`, and `QueryRaw` all resolve the tenant from `ctx` (`tenant.Require`)
  and run through the tenant-stamped transaction path (`inTenantTx` /
  `inDynamicTenantTx`) — the same three-gate backstop (RLS policy + the
  grove tenant-scoping hook + a `tenant_id` predicate) that protects every
  other tenant table, with no bespoke isolation logic written for this
  feature.
- **D3 — One port, three methods.** `query.AnalyticsQuerier`
  (`core/query/analytics.go`), exposed via `f.Analytics()`:
  - `Track(ctx, events []AnalyticsEvent) error` — bulk, outbox-bypass ingest
    of schemaless customer events, mirroring `TSQuerier.BulkWrite`. One
    multi-row `INSERT` per call; an optional `DedupKey` gives idempotent
    retries via a partial unique index. Dedup keys are keyed
    `(tenant_id, dedup_key)` — tenant-wide, not per-scope — so two scopes
    reusing one key collide and only the first event lands.
  - `Query(ctx, AnalyticsQuery, into any) error` — an engine-neutral cube
    aggregation (`Measures` × `Dimensions` × `TimeBucket`, filtered by the
    existing `Where` vocabulary), computed on-demand.
  - `QueryRaw(ctx, into any, sql string, args ...any) error` — a read-only
    SQL escape hatch for aggregations the cube can't express, guarded by a
    statement precheck plus a genuine Postgres `READ ONLY` transaction.
  - `notConfiguredAnalytics{}` degrades every method to
    `ErrStoreNotConfigured` when `Config.Insights.Enabled` is false, the same
    discipline as every other optional port.
- **D4 — `InsightsSpec`/`MetricSpec`: deny-by-default opt-in, no
  redaction.** An entity is projected into `fabriq_insights_facts` only if
  its `registry.EntitySpec.Insights` is non-nil (`registry.InsightsSpec{
  Measures, Dimensions }`); a spec naming neither is rejected at
  registration (a marked-but-empty spec would silently project nothing).
  Unlike `AnalyticsSpec` (ADR 0013), there is **no `Include`/`Hash`/
  `IncludeAll` distinction** — the projected columns are exactly the
  declared `Measures ∪ Dimensions`, with no field minimization, because the
  data never leaves the tenant's own database. `MetricSpec` optionally
  declares a named cube query (`Name`, `Source`, `Measures`, `Dimensions`,
  `DefaultBucket`); it is validated at registration (non-empty `Name`, at
  least one measure, measure fields are real columns) but — see Known gaps
  below — is not yet resolvable by name at query time.
- **D5 — `proj:insights`, a per-tenant consumer on the shared stream.**
  `core/insights.Consumer` reads the same shared Redis event stream
  (`registry.StreamKey()`) that graph, search, and the ADR 0013 sink already
  read, through the same exported `projection.Source` seam — no new
  transport. For each envelope, a pure `core/insights.Applier` (deterministic,
  canonical-JSON, so live apply and backfill would agree byte-for-byte)
  produces a `Fact` if the entity carries an `InsightsSpec`, and the write
  lands through a `FactSink` resolved **per ctx-tenant** from the shard
  router (`shard.NewAnalytics`), landing on that tenant's own shard/database
  through the same `inTenantTx`/RLS path every other per-tenant write uses.
  This is the structural opposite of `proj:analytics` (ADR 0013), which
  writes every tenant into one shared store. The consumer is supervised
  (worker and catalog sweeper planes alike) only when `Config.Insights.Enabled`
  is true **and** at least one registered entity carries an `InsightsSpec`;
  otherwise it does not start (and a warning is logged if `Insights.Enabled`
  is on with no entity marked, since nothing would flow).

## Trust boundary — hard boundary vs. ADR 0013

Tenant analytics (this ADR) and the operator sink (ADR 0013) are
deliberately separate capabilities that happen to share two patterns — the
`projection.Source` consumer seam and deny-by-default registry opt-in — and
nothing else:

| | Operator sink (`core/analytics`, ADR 0013) | Tenant analytics (`core/insights`, this ADR) |
|---|---|---|
| Audience | Operators only | The tenant's own app/users |
| Scope | Cross-tenant, one shared store | Single-tenant, one store per tenant |
| RLS | None (deliberately — see ADR 0013) | **Enforced**, inherited from the tenant DB |
| Store | Separate analytics DB, DSN validated distinct at boot | **In-tenant**: same DB/schema/shard as domain data, no DSN |
| Facade surface | admin API `POST /admin/analytics/query` (`pganalytics.QueryReadOnly`) | **`f.Analytics()`** (`Track`/`Query`/`QueryRaw`) |
| Redaction | Field-minimized allow-list (`AnalyticsSpec.Include`/`Hash`) | **None** — the tenant owns its own data |
| Registry opt-in | `EntitySpec.Analytics *AnalyticsSpec` | `EntitySpec.Insights *InsightsSpec` (+ optional `Metrics []MetricSpec`) |
| Consumer group | `proj:analytics` → one shared store | `proj:insights` → each tenant's own store |
| Tables | `fabriq_analytics_facts` / `_events` / `_applied` (operator DB) | `fabriq_insights_facts` / `_events` (in-tenant, RLS) |

**Hard invariant, enforced structurally, not just by convention:**

1. **No shared table, DSN, consumer group, or query path.** `core/insights`
   does not import `core/analytics`, and `adapters/postgres/insights*.go`
   does not import `core/analytics` either — verified by
   `grep -rn "core/analytics" core/insights adapters/postgres/insights*.go`
   returning no matches. The two subsystems are wired independently in
   `open.go` (`Stores.AnalyticsConsumer` vs. `Stores.InsightsConsumer`) and
   configured independently (`Config.Analytics` vs. `Config.Insights`).
2. **The operator sink is never wired to a tenant surface.** `f.Analytics()`
   (this ADR's port) has no path to `core/analytics.Sink` or the
   `fabriq_analytics_*` tables; a tenant's own application can never reach
   the operator store through the facade.
3. **Tenant analytics is never cross-tenant.** Every `core/insights` write and
   read is scoped by `ctx`'s tenant through `tenant.Require`/`inTenantTx`/
   `inDynamicTenantTx`, and both tables carry the same RLS policy as every
   other tenant table. There is no method, flag, or admin surface that reads
   `fabriq_insights_events`/`fabriq_insights_facts` across tenants — unlike
   the operator sink, which exists precisely to do that in its own separate
   store.

## Known gaps (phase 1)

These are deliberate phase-1 scope cuts, not accidental omissions — recorded
here so they are never mistaken for coverage:

- **`AnalyticsQuery.Having` is not implemented.** Post-aggregation filtering
  is rejected with an explicit error (`buildInsightsSQL` returns
  `"fabriq: insights Having is not implemented yet"`) rather than being
  silently ignored. Pre-aggregation `Filter` works today.
- **Declared `MetricSpec`s do not yet resolve by name in `Query`.** A
  `MetricSpec` is validated at registration (name required, at least one
  measure, measure fields must be real columns), making it discoverable
  metadata and a phase-2 rollup candidate — but `AnalyticsQuery.Source` is
  currently always treated as either a customer event `Name` or, in the
  future, a projected entity name; passing a declared metric's `Name` as
  `Source` does **not** expand it into that metric's `Measures`/`Dimensions`/
  `DefaultBucket`. Callers must spell out the full cube query today.
- **The cube `Query` reads customer events only.** `buildInsightsSQL` always
  selects `FROM fabriq_insights_events`. Domain entities marked with
  `InsightsSpec` **are** projected into `fabriq_insights_facts` by the
  `proj:insights` consumer (D5 above) — but querying those projected facts
  through the cube (`Source` naming an entity instead of an event) is **not
  wired**. This is a fast-follow, not a silent partial feature: the
  projection pipeline is complete and tested end-to-end, only the read side
  of "query an opted-in entity by name" is outstanding.
- **No percentile measures.** Phase-1 `Measure.Kind` is `count`/`sum`/`avg`/
  `min`/`max`/`count_distinct` only; `p95`/percentile aggregation (mentioned
  as an open item in the design) is not implemented.
- **Numeric range filters are numeric only when the bound value is numeric.**
  `Gt`/`Gte`/`Lt`/`Lte` against a JSONB prop cast to `::numeric` when the
  filter's Go value is a numeric type (`mapCondToProp`/`isNumericValue`) —
  this is implemented and correct. A range filter with a **string** value
  compares lexicographically (`props ->> 'key'` as text) — this is expected,
  by-design behavior for string-typed dimensions, not a bug, but worth
  stating explicitly since it is easy to assume numeric semantics
  everywhere.
- **No materialized rollups.** The cube computes every `Query` on-demand;
  there is no continuous-aggregate or rollup-table routing. The
  `(Measures, Dimensions, TimeBucket)` shape of `AnalyticsQuery` is exactly
  what a phase-2 rollup would key on, so this is designed to need no API
  change when it lands — it is simply not built in phase 1.
- **Eventful promotion, per-tenant OLAP backend, and retention** (listed as
  phase-3 "later" items in the design) are not built: `Track` always
  bypasses the outbox; there is no pluggable backend behind
  `query.AnalyticsQuerier` other than the Postgres adapter; and there is no
  retention/pruning control on `fabriq_insights_events` (contrast the
  operator sink's `eventRetention`/`partitionEvents`).

## Consequences

- Tenant-facing analytics has exactly one supported home
  (`f.Analytics()`/`core/insights`), symmetric with, but structurally
  disjoint from, the operator sink's one supported home
  (`core/analytics`/ADR 0013). Neither can be reached from the other's
  surface.
- Turning it on costs one config flag (`Config.Insights.Enabled`) and
  per-entity opt-in markings — no new database, no DSN, no boot-time
  collision check, because the store rides the tenant's own connection and
  migration chain like every other fabriq table.
- Because RLS and structural tenant stamping are inherited rather than
  reimplemented, tenant analytics gets fleet-wide isolation guarantees for
  free — the same three-gate backstop the rest of fabriq relies on, with no
  new isolation code to review or drift from.
- The `(Measures, Dimensions, TimeBucket)` query contract is deliberately
  rollup-ready: phase 2 (materialized rollups / Timescale continuous
  aggregates) and later projected-fact querying can land as adapter-internal
  changes with no port signature change.
- The Known gaps section above is the authoritative phase-1 boundary; the
  fast-follows called out there (`Having`, declared-metric resolution,
  projected-fact querying via the cube, percentiles, rollups) are the
  documented next increments, not silently half-wired features.

Related: ADR 0013 (`0013-cross-tenant-analytics-sink.md`, the operator-only
counterpart this is deliberately separate from); design spec
`docs/superpowers/specs/2026-07-10-per-tenant-analytics-design.md`.

Customer guide: `(data-planes)/analytics.mdx`.
