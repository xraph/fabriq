# Fabriq Data Fabric Implementation Plan

> **STATUS (2026-06-12): Phases 0–3 fully implemented, tested and
> integration-verified (E2E: command → outbox → relay → stream → hub →
> SSE delta with Last-Event-ID resume). Phases 4–7 scaffolded per spec
> (interfaces, dialect translator with unit tests, conformance suite,
> contracts as code comments, TODO tests) — awaiting direction review.
> Deviations and discoveries recorded in docs/decisions/0001–0006.**

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `github.com/xraph/fabriq` — the TWINOS data fabric: the only module allowed to talk to any datastore, enforcing "every write emits one versioned event; every access is tenant-scoped; projections are rebuildable from Postgres."

**Architecture:** Hexagonal kernel (`core/` — pure, registry-driven, port interfaces) + swappable adapters (`adapters/` — the ONLY packages importing raw drivers) + a TWINOS domain pack (`domain/`). Writes flow through a command executor (grove tx → row write → transactional outbox → commit); a leader-elected relay publishes to Redis Streams; projections + subscription hub consume streams. Binaries are Forge apps; the CLI is forge/cli.

**Tech Stack:** Go 1.25.7 · grove v1.5.2 (pgdriver, pgmigrate, hook engine, schema registry) · forge v1.6.9 (app + cli) · go-redis/v9 · oklog/ulid/v2 · testcontainers-go · prometheus client · OpenTelemetry · golang-jwt/v5 (api-example only).

**Naming:** product/module/binaries = **fabriq** (`fabriq` CLI, `fabriq-worker`, `api-example`); the facade interface keeps the conceptual name `Fabric` (in `core/query`); tables prefixed `fabriq_`; event stream `fabriq:events`; change channels `changes:{tenant}:{scope}:{id}`.

---

## Verified upstream facts (do not re-derive)

- `pgdriver.New().Open(ctx, dsn, opts...)` → `grove.Open(pgdb)`; `db.Driver().(*pgdriver.PgDB)` recovers the typed driver.
- **Transactions:** `PgDB.BeginTxQuery(ctx, *driver.TxOptions) (*PgTx, error)`; `PgTx` exposes `NewSelect/NewInsert/NewUpdate/NewDelete/NewRaw` + `Commit/Rollback`. Raw SQL inside tx: `PgTx.NewRaw("SET LOCAL app.tenant_id = ...")`.
- **Hooks:** `hook.PreQueryHook.BeforeQuery(ctx, *hook.QueryContext) (*hook.HookResult, error)`; Deny aborts. ~~grove.Open does NOT propagate; PgTx drops hooks~~ **FIXED upstream (grove a01144a, 2026-06-12): Open propagates to SetHooks-capable drivers, PgTx carries hooks, `QueryContext.InTransaction` marks the path, `Conditions` populated.** Backstop policy: allow in-tx (RLS guards), deny pool-path (ADR-0002).
- **Migrations:** `migrate.NewGroup(name, opts...)`, `group.MustRegister(&migrate.Migration{Name, Version, Up, Down})`, `MigrateFunc = func(ctx, migrate.Executor) error`; executor for pg via `import _ ".../pgdriver/pgmigrate"` then `migrate.NewExecutorFor(pgDB)`; `migrate.NewOrchestrator(exec, groups...)` → `Migrate/Rollback/Status`; built-in lock table (`AcquireLock`).
- **Forge app:** `forge.New(opts...)`; `app.Router().GET/POST/SSE(path, handler, opts...)`; handler `func(ctx forge.Context) error`; `ctx.WriteSSE(event, data)`; health endpoints `/_/health`, `/_/livez`, `/_/readyz`, metrics `/_/metrics`; `RunnableExtension{Run(ctx), Shutdown(ctx)}` for background loops.
- **Forge CLI:** `cli.New(cli.Config{Name, Version, Description})`, `cli.NewCommand(name, desc, func(ctx cli.CommandContext) error)`, `cmd.AddFlag(cli.NewStringFlag(...))`, `c.AddCommand(cmd)`, `c.Run(os.Args)`.
- **grove kv redisdriver exists but its stream API is thin; fabriq's redis adapter uses go-redis/v9 directly (allowed by spec, lint-fenced to adapters) — ADR-0003.**

---

## Phase 0 — Repo scaffold

### Task 1: Module scaffold + lint boundaries + CI

**Files:** `go.mod`, `doc.go`, `.golangci.yml`, `Makefile`, `.gitignore`, `.github/workflows/ci.yml`, `.github/dependabot.yml`, `README.md`

- [ ] go.mod: `module github.com/xraph/fabriq`, `go 1.25.7`.
- [ ] `.golangci.yml` per prime-gp template **plus depguard rules**:
  - deny `github.com/xraph/grove/drivers/*`, `github.com/redis/go-redis/*`, `github.com/elastic/go-elasticsearch/*`, FalkorDB clients everywhere EXCEPT `adapters/**` (and `migrations/**` may import `pgmigrate` for executor registration; `fabriqtest/**` may import drivers for containers).
  - deny `github.com/xraph/fabriq/domain` and `github.com/xraph/forge*` inside `core/**` (core is framework-free).
- [ ] Makefile targets: `build test test-integration bench lint fmt tidy cover`.
- [ ] Verify: `go build ./...` (empty module + doc.go), commit `chore: scaffold fabriq module`.

---

## Phase 1 — Kernel (pure Go, unit tests + benchmarks, no Docker)

### Task 2: Error taxonomy (`errors.go`)

Typed errors + helpers, all `errors.Is`-able:
```go
var (
    ErrNoTenant          = errors.New("fabriq: no tenant in context")
    ErrNotFound          = errors.New("fabriq: not found")
    ErrProjectionLag     = errors.New("fabriq: projection lagging")
    ErrTenantHookTripped = errors.New("fabriq: tenant guard tripped")
)
type VersionConflictError struct{ Aggregate, AggID string; Expected, Actual int64 }
func (e *VersionConflictError) Error() string
var ErrVersionConflict = errors.New("fabriq: version conflict") // VersionConflictError.Is target
```
TDD: `errors_test.go` — `errors.Is` round-trips, message formats.

### Task 3: Tenancy (`core/tenant`)

```go
func WithTenant(ctx context.Context, tenantID string) context.Context
func FromContext(ctx context.Context) (string, error) // ErrNoTenant if absent/empty
func MustFromContext(ctx context.Context) string      // panics (internal use)
// guard.go
func Require(ctx context.Context) (string, error)     // the single structural enforcement point
```
Tenant IDs validated (`^[a-zA-Z0-9_-]{1,64}$`) at stamp time so derived names (graph/index/stream) are always safe. TDD: stamp/recall, missing → ErrNoTenant, invalid ID rejected. Bench: `BenchmarkFromContext`.

### Task 4: Schema registry (`core/registry`)

`spec.go` — the declarative contract (Kind + CRDTSpec exist from phase 1):
```go
type Kind int
const (KindAggregate Kind = iota; KindDocument)
type Scope string                 // subscription scope name, e.g. "id","site","tenant"
const (ScopeByID Scope = "id"; ScopeByTenant Scope = "tenant")
type EdgeSpec struct{ Field, Rel, Target string }
type SearchSpec struct{ Index string; Fields []string }
type CRDTSpec struct{ Engine string; SnapshotEvery int; QuietWindow time.Duration }
type EntitySpec struct {
    Name string; Kind Kind; Model any // grove-tagged struct pointer
    GraphNode string; Edges []EdgeSpec
    Search *SearchSpec; Subscribe []Scope; CRDT *CRDTSpec
}
```
`registry.go` — `New()`, `Register(spec) error`, `MustRegister`, `Get(name)`, `All()`, `Validate() error` (startup: unique names, model bound, edges reference registered targets, document kinds need CRDT spec).
`model.go` — binds `Model` via `grove/schema.Registry` → table name, columns, pk, tenant column presence, version column presence (aggregates MUST have `id`,`tenant_id`,`version` columns), edge `Field`s must be real columns.
`derive.go` — pure derivations: `ChannelName(tenant, scope, id)`, `EventType(entity, verb)`, `GraphLabel`, `SearchIndex(tenant)`, `StreamKey()`; all tenant-scoped names derived HERE only.
TDD: table tests for validation failures (dup name, unbound model, bad edge target, missing version column), derivation goldens. Bench: `BenchmarkRegistryGet`, `BenchmarkDeriveChannel`.

### Task 5: Events (`core/event`)

```go
type Envelope struct {
    ID string; TenantID string; Aggregate string; AggID string
    Version int64; Type string; At time.Time
    PayloadSchemaVersion int; Payload json.RawMessage; Traceparent string
}
func NewID() string // ULID, crypto-rand monotonic
```
`codec.go` — JSON encode/decode + field validation. `upcast.go`:
```go
type Upcaster struct{ Type string; FromVersion int; Fn func(json.RawMessage) (json.RawMessage, error) }
type UpcasterChain struct{ ... } // Register, Apply(env) — ordered vN→vN+1 at decode
```
TDD: codec round-trip, ULID ordering, chain applies in order / gaps error / latest passthrough. Bench: encode/decode, 3-step chain.

### Task 6: Projection mutations + appliers (`core/projection`)

`mutation.go` — engine-neutral sum type: `NodeUpsert{Label, ID, Props}`, `EdgeUpsert{Rel, FromLabel, FromID, ToLabel, ToID, Props}`, `NodeDelete`, `EdgeDelete`, `DocIndex{Index, ID, Doc}`, `DocDeindex`.
`applier.go` — `type Applier interface{ Apply(event.Envelope) ([]Mutation, error) }` + `GraphApplier(reg)`/`SearchApplier(reg)` derived from registry: `<entity>.created|updated` → NodeUpsert + EdgeUpsert per EdgeSpec (nil/empty FK → EdgeDelete), `.deleted` → NodeDelete/DocDeindex. Appliers NEVER emit dialect strings.
TDD: table tests (create w/ both edges, update clearing FK, delete, unknown entity → no-op, version prop always present). Bench: `BenchmarkGraphApplier`.
(`engine.go`, `state.go`, `rebuild.go`, `reconcile.go` are Phase 4 skeletons — interfaces + TODO tests now.)

### Task 7: Command plane core (`core/command`)

```go
type Op int
const (OpCreate Op = iota; OpUpdate; OpDelete)
type Command struct {
    Entity string; Op Op; AggID string
    Payload any            // grove model instance for create/update
    ExpectedVersion *int64 // optimistic concurrency
}
type Result struct{ AggID string; Version int64; EventID string }

// Ports implemented by adapters/postgres (and fabriqtest fakes):
type Store interface { InTenantTx(ctx context.Context, fn func(Tx) error) error }
type Tx interface {
    CurrentVersion(ctx context.Context, spec *registry.EntitySpec, aggID string) (int64, error) // 0 = absent
    ApplyChange(ctx context.Context, spec *registry.EntitySpec, op Op, aggID string, version int64, payload any) error
    AppendOutbox(ctx context.Context, env event.Envelope) error
}
type Executor struct{ ... } // New(reg, store, opts) 
func (x *Executor) Exec(ctx context.Context, cmd Command) (Result, error)
func (x *Executor) ExecBatch(ctx context.Context, cmds []Command) ([]Result, error) // one tx
```
Behavior: tenant required → spec lookup (KindAggregate only) → validate payload against spec/model → ULID AggID on create → version read → conflict check (`*VersionConflictError`) → ApplyChange(version+1) → exactly ONE outbox envelope (`<entity>.<created|updated|deleted>`, traceparent from ctx) → Result. Batch = same tx, ordered, all-or-nothing.
`version.go` (conflict calc), `validate.go` (required cols non-zero, payload type matches spec.Model), `outbox.go` (envelope construction from command).
TDD against fakes: create/update/delete happy paths, no tenant → ErrNoTenant, wrong expected version → conflict, document kind rejected, batch atomicity (fail mid-batch → nothing applied), exactly-one-event-per-command. Bench: `BenchmarkExec_Fake`, `BenchmarkExecBatch100_Fake`.

### Task 8: Query ports + subscription hub (`core/query`, `core/subscribe`)

`query/ports.go` — the facade contract (engine-neutral):
```go
type Fabric interface {
    Exec(ctx context.Context, cmd command.Command) (command.Result, error)
    ExecBatch(ctx context.Context, cmds []command.Command) ([]command.Result, error)
    Relational() RelationalQuerier
    Graph() GraphQuerier
    Search() SearchQuerier
    Timeseries() TSQuerier
    Vector() VectorQuerier
    Document() document.Store
    Subscribe(ctx context.Context, scope SubscribeScope) (<-chan Delta, error)
    WaitForProjection(ctx context.Context, projection, aggregate, aggID string, version int64) error
}
type RelationalQuerier interface {
    Get(ctx context.Context, entity, id string, into any) error
    GetMany(ctx context.Context, entity string, ids []string, into any) error // ONE batched query
    List(ctx context.Context, entity string, q ListQuery, into any) error
    Query(ctx context.Context, into any, sql string, args ...any) error // raw escape hatch (hook-guarded)
}
type GraphQuerier interface {
    Query(ctx context.Context, cypher string, params map[string]any, into any) error
    TraverseAndHydrate(ctx context.Context, cypher string, params map[string]any, into any) error
    ApplyMutations(ctx context.Context, target string, muts []projection.Mutation) error
}
type SearchQuerier interface { Search(ctx, q SearchQuery, into any) error; ApplyMutations(...) }
type TSQuerier interface { BulkWrite(ctx context.Context, series string, points []Point) error; Range(ctx, q RangeQuery, into any) error }
type VectorQuerier interface { Upsert(ctx, ...) error; Similar(ctx, q VectorQuery, into any) error }
```
`delta.go` — `Delta{Channel, EventID, TenantID, Aggregate, AggID, Version, Type, At, Payload}`.
`compose.go` — `TraverseAndHydrate` helper: graph query → IDs → `RelationalQuerier.GetMany` (never N+1).
`subscribe/` — `channel.go` (scope→channel via registry derive, server-side only), `authz.go` (`AuthzFunc(ctx, scope) error` hook), `conflate.go`:
```go
type Conflator struct{ ... } // New(window time.Duration, flush func([]query.Delta))
func (c *Conflator) Offer(d query.Delta) // LWW per (channel, aggregate, aggID)
```
`hub.go` — subscriber registry, per-channel fan-out feeding conflators, `StreamSource` port (implemented by redis adapter in phase 3; fake in fabriqtest). Conflation applies to delta channels only — the hub exposes a raw (non-conflating) attach for the future CRDT sub-protocol.
`sse.go` — stdlib-only SSE writer: explicit `http.Flusher` after every event, `id:` = stream ID, `Last-Event-ID` parse helper, heartbeat comments.
TDD: channel resolution (client never names channels), authz deny, conflator LWW + flush window (fake clock), hub fan-out, SSE wire format golden incl. flush calls (recorder implementing Flusher), Last-Event-ID resume parse. Bench: `BenchmarkConflatorOffer`, `BenchmarkHubPublish_1kSubs`.

### Task 9: Document plane port + fabriqtest kit

`core/document/store.go`:
```go
type Materialized struct{ DocID string; Snapshot json.RawMessage; Version int64 }
type Store interface {
    ApplyUpdate(ctx context.Context, docID string, update []byte) error
    Sync(ctx context.Context, docID string, stateVector []byte) ([]byte, error)
    Snapshot(ctx context.Context, docID string) (Materialized, error)
    Compact(ctx context.Context, docID string) error
}
```
+ `DESIGN.md` (append-only crdt_updates, quiet-window materialization → ONE domain event, post-merge validation, grove crdt engine reference, awareness via pub/sub never persisted), `materialize.go`/`compact.go` TODO stubs.
`fabriqtest/` — `fakes.go`: `FakeStore` (command.Store w/ versions+outbox slice), `FakeRelational`, `FakeGraph` (in-mem nodes/edges + applied-mutation log + canned query responses), `FakeSearch`, `FakeStreams` (in-mem streams/groups implementing relay sink + hub source), `FakeDocumentStore` (TODO errs). `fixtures.go`: seeded tenants + a `testdomain` (in fabriqtest, NOT TWINOS) registry. Fake behavior pinned by the same contract tests adapters will run.

### Task 10: Facade + config (`fabriq.go`, `options.go`, `config.go`)

`config.go` — declarative struct (yaml-tagged; future `fabriqd` loads it): stores (postgres/redis/falkordb/elastic DSNs), projections (enabled, consumer group names), subscriptions (conflation window, maxlen), entities implicit via Register.
`options.go` — `WithConflationWindow`, `WithStreamMaxLen`, `WithLogger`, `WithMeterRegistry`, `WithClock` etc.
`fabriq.go` — `Open(ctx, cfg, opts...) (*Fabriq, error)` wiring registry + adapters (postgres+redis in phase 3; graph/search return typed "not configured" errors until phases 4/5); implements `query.Fabric`. `RegisterAll` hook for domain packs.
TDD: Open with fakes (test seam `withAdapters` internal option), unconfigured port → ErrStoreNotConfigured, facade satisfies `query.Fabric` (compile assertion).

---

## Phase 2 — Postgres adapter, migrations, command plane integration (testcontainers)

### Task 11: Migrations (`migrations/`)

grove group `fabriq` + ordered migrations:
- `0001_outbox.go` — `fabriq_outbox(id text pk, tenant_id, aggregate, agg_id, version bigint, type, at timestamptz, payload_schema_version int, payload jsonb, traceparent text, published_at timestamptz null)` + `UNIQUE(tenant_id, aggregate, agg_id, version)` + partial index `WHERE published_at IS NULL` + `NOTIFY fabriq_outbox` via `pg_notify` in executor (no trigger needed).
- `0002_projection_state.go` — `fabriq_projection_state(tenant_id, projection, model_version int, event_version text, status, target_name, updated_at, PRIMARY KEY(tenant_id, projection))`.
- `0003_site_asset_tag.go` — domain tables (sites, assets w/ site_id+parent_id, tags w/ asset_id) all with `id, tenant_id, version, created_at, updated_at` + FK indexes. `tag_readings(time timestamptz, tenant_id, tag_id, value double precision, quality int)`.
- `0004_rls_policies.go` — `ALTER TABLE ... ENABLE ROW LEVEL SECURITY; CREATE POLICY tenant_isolation ... USING (tenant_id = current_setting('app.tenant_id', true))` on every tenant table + `FORCE` RLS.
- `0005_timescale.go` — `create_hypertable('tag_readings','time')` + compression policy (guarded: skip if extension absent → log NOTICE; integration image has it).
- `0006_pgvector.go` — `CREATE EXTENSION IF NOT EXISTS vector`, `fabriq_embeddings` table, `CREATE INDEX CONCURRENTLY` (executor runs outside tx — verify pgmigrate behavior; if tx-wrapped, document + use lock-safe fallback).
- `0007_crdt_updates.go` — `fabriq_crdt_updates(doc_id, seq bigserial, tenant_id, update bytea, at)` + `fabriq_crdt_snapshots`.
- `0008_leases.go` — `fabriq_leases(role text pk, holder text, lease_until timestamptz)`.
Integration test: `migrate up` on testcontainer → `Status` all applied → `down` reverts last.

### Task 12: Postgres adapter (`adapters/postgres`)

- `adapter.go` — `Open(ctx, dsn, reg, opts) (*Adapter, error)`: pgdriver+grove, `SetHooks` with backstop engine; implements `query.RelationalQuerier` (Get/GetMany via `WHERE id = ANY`, List, raw Query) — every query tenant-predicated structurally (`tenant_id = $`) AND hook-asserted.
- `tx.go` — implements `command.Store`: `BeginTxQuery` → `SET LOCAL app.tenant_id` (parameterized via `set_config($1,$2,true)`) → fn → commit. `Tx` methods use PgTx builders; `CurrentVersion` uses `FOR UPDATE`.
- `hooks.go` — `TenantBackstop` PreQueryHook: table in registry's tenant tables + no `tenant_id` condition → Deny + `ErrTenantHookTripped` + trip counter.
- `timescale.go` — `TSQuerier`: `BulkWrite` multi-row insert batches (event-bypass path), `Range`.
- `vector.go` — `VectorQuerier` on pgvector (`<=>` cosine).
- Registry-conformance test: apply migrations → diff `information_schema.columns` vs every registered spec's model columns → fail on drift.
Integration tests (tag `integration`, image `timescale/timescaledb-ha:pg17`): RLS isolation (tenant A cannot read B even with raw SQL through adapter), hook trips on unpredicated pool query, CurrentVersion/FOR UPDATE serialization, GetMany single-query (assert via pg_stat or query log), bulk telemetry write. Bench (integration): `BenchmarkExecCommand_PG`, `BenchmarkBulkWrite_PG`.

### Task 13: Command plane end-to-end on PG + CLI migrate

Wire `command.Executor` + pg Store. Integration: create→update→delete emits 3 outbox rows w/ versions 1,2,3; concurrent update conflict; batch atomicity (induced failure → zero rows). 
`cmd/fabriq` (forge/cli): `migrate up|down|status` (grove orchestrator, lock held), `inspect registry|state`. Manual smoke documented in README.

---

## Phase 3 — Redis, relay, hub, api-example

### Task 14: Redis adapter (`adapters/redis`)

- `streams.go` — `Publisher` (XADD w/ MAXLEN~ to `fabriq:events` + each derived change channel), `Consumer` (XREADGROUP + XAUTOCLAIM loop, ack), `HubSource` (XREAD change channels from last-ID for SSE resume).
- `cache.go` — versioned key prefixes (`{prefix}:v{N}:{tenant}:{entity}:{id}`).
- `pubsub.go` — ephemeral presence (future CRDT rooms).
Integration tests (redis:7-alpine): publish→group consume→ack, MAXLEN trim, resume from last-ID, two groups independent cursors.

### Task 15: Outbox relay + leadership (`adapters/postgres/relay.go`, `internal/leader`)

- `internal/leader` — lease-table leadership (`fabriq_leases`, renew loop, lost-lease callback). ADR-0004 (lease vs session advisory lock under pools).
- `relay.go` — poll `FOR UPDATE SKIP LOCKED WHERE published_at IS NULL ORDER BY id LIMIT batch` + LISTEN `fabriq_outbox` wake (grove raw conn; fall back to interval poll) → Publisher → mark published. At-least-once; crash-replay test.
Integration: command → outbox → relay → stream visible; duplicate-safe; relay only on leader. Bench: relay throughput 1k events.

### Task 16: Hub wiring + WaitForProjection + worker binary

- Hub ← redis HubSource; `WaitForProjection` polls `fabriq_projection_state`/aggregate version w/ deadline → `ErrProjectionLag`.
- `cmd/fabriq-worker` — forge app; extensions: relay runner (leader), projection consumers (skeleton consumers for now — real appliers land phase 4/5), reconciler placeholder; `/_/livez`,`/_/readyz`,`/_/metrics`; SIGTERM drain test (unit-level on runner).

### Task 17: api-example (forge app)

- `auth.go` — HS256 JWT middleware (secret from config), claims→`tenant.WithTenant` (never forwarded headers).
- `handlers.go` — `POST /assets` etc. (command), `GET /assets/:id` (relational), `GET /assets` (list).
- `sse.go` — fetch-then-subscribe: snapshot + `Last-Event-ID`-resumed deltas via core SSE writer (explicit flush; works behind proxy).
E2E integration test: PG+Redis containers, worker relay in-process, HTTP server: login-less signed JWT → create asset → SSE receives conflated delta → resume with Last-Event-ID. This is the phase-3 acceptance gate.

---

## Phases 4–7 — Scaffolds (interfaces, skeletons, TODO tests, then STOP)

### Task 18: Graph/search/projection-engine scaffolds
- `adapters/falkordb/` — adapter skeleton over go-redis `GRAPH.QUERY` (dialect in `mutate.go` only), `routing.go` (tenant→`tenant_{id}` + blue-green `_v{N}`); compile-ready, TODO tests.
- `adapters/graphtest/suite.go` — exported openCypher conformance table (MERGE node/edge, parameterized match, delete, var-length path) — runnable against any GraphQuerier; wired for falkordb w/ skip-if-no-docker.
- `adapters/elastic/` — skeleton (no client dep yet beyond interface), `index.go` alias-swap plan.
- `core/projection/engine.go|state.go|rebuild.go|reconcile.go` — full interfaces + skeleton impls + TODO tests; `cmd/fabriq rebuild|reconcile` commands registered returning "not implemented".
- `internal/otel` (traceparent inject/extract — REAL, used by envelope already), `internal/metrics` (prometheus counters/gauges: outbox backlog, hook trips, conflation depth, projection lag — registered; lag wired in phase 6).
- `core/document` migrations already in 0007; skeleton tests with TODOs.
- Docs: `docs/MIGRATIONS.md`, `docs/OPERATIONS.md`, ADRs 0001..0005.

### Task 19: Final verification + summary
- `make lint && make test` green; `make test-integration` green (Docker); `make bench` runs.
- README quickstart; STOP and summarize for direction review (per spec).

---

## ADR index (docs/decisions/)
- 0001: fabriq is a standalone Forge-ecosystem repo, module `github.com/xraph/fabriq`; twinos consumes it.
- 0002: tenant enforcement layering — structural stamping + RLS (`SET LOCAL`) primary in tx path; grove hook backstop denies pool path. Grove hook gaps FIXED upstream (a01144a): hooks fire in tx with InTransaction flag; backstop allows that path explicitly.
- 0003: go-redis/v9 directly in adapters/redis (grove kv stream API insufficient: no MAXLEN/XAUTOCLAIM); fenced by depguard.
- 0004: lease-table leadership instead of session advisory locks (pooled connections can't hold session locks safely).
- 0005: SSE bridge is stdlib-only in core/subscribe (forge `WriteSSE` lacks `id:`/Last-Event-ID control); forge apps mount it as a raw handler.
