# AGENTS.md

Fabriq is a data fabric for Go. Every write is a command that commits once, in a Postgres transaction, together with a versioned event in a transactional outbox. A leader-elected relay publishes those events to Redis Streams, and from there the FalkorDB graph, the Elasticsearch index, the caches and live subscribers catch up. Reads go through typed capability ports. Every call is tenant-scoped, and the graph and search projections can always be rebuilt from Postgres. You get it as a library, a `fabriq` binary (worker plus operator CLI), a container image and a Helm chart.

## Modules

There are five Go modules, and `release-dispatch.yml` tags all of them at the same version.

| Module | Directory | Tag |
|---|---|---|
| `github.com/xraph/fabriq` | `.` | `vX.Y.Z` |
| `github.com/xraph/fabriq/core` | `core/` | `core/vX.Y.Z` |
| `github.com/xraph/fabriq/remote/grpc` | `remote/grpc/` | `remote/grpc/vX.Y.Z` |
| `github.com/xraph/fabriq/adapters/wardenauthz` | `adapters/wardenauthz/` | `adapters/wardenauthz/vX.Y.Z` |
| `github.com/xraph/fabriq/remote/grpc/cmd/fabriqserver` | `remote/grpc/cmd/fabriqserver/` | `remote/grpc/cmd/fabriqserver/vX.Y.Z` |

`core` is the kernel: ports, the schema registry, the command plane, events, tenancy. It depends on nothing heavier than grove, so a consumer that only needs entity specs and interfaces can take it alone. The root module requires `core` itself.

Every module carries `replace` lines pointing back inside this repo (`./core`, `../..`, `../../core` and so on), so local builds compile your working tree. They are intentional. A consumer's build ignores every one of them and resolves the require versions instead, which is why those versions matter so much at release time.

All five `go` directives have to match. The release and `scripts/check-module-versions.sh` both fail if one drifts (`go mod edit -go=<root's version>` fixes it).

## Commands

Prefix go commands with `GOWORK=off` to get CI's module graph.

```bash
GOWORK=off go build ./...
make test              # go test -race -count=1 ./... in the root, then in core/
make test-integration  # -tags=integration -p 1 -timeout 30m ./... in the root (Docker)
make test-all          # both
make lint              # golangci-lint run --build-tags=integration --timeout 10m (root only)
make lint-fix
make fmt               # gofmt -s -w . then goimports -local github.com/xraph/fabriq
make vet
make check             # fmt, vet, lint
make tidy              # go mod tidy and verify, root and core only
make cover             # coverage.out in the root and in core/
make bench             # or make bench-smoke for a single pass
make build             # go build ./... then bin/fabriq from ./cmd/fabriq
make run               # go run ./cmd/fabriq serve
```

`./...` does not cross into a nested module. To test `remote/grpc`, `fabriqserver` or `wardenauthz`, `cd` into it and run `GOWORK=off go test ./...` there.

Container and chart targets: `make vendor`, `make docker-build` (vendors first, then builds `deploy/docker/Dockerfile`), `make helm-lint`, `make helm-template`, `make helm-package`.

Two read-only scripts are worth running before a release:

```bash
./scripts/check-module-versions.sh   # placeholders, go-directive drift, tag skew
./scripts/release-modules.sh 1.8.0   # preflight: prints the release plan, changes nothing
```

`release-modules.sh` takes the version without the `v` and refuses a dirty tree, untracked files included. A `go build` inside `remote/grpc/cmd/fabriqserver` leaves a `fabriqserver` binary there that `.gitignore` doesn't cover, and that alone is enough to stop it. Don't commit that binary.

`./updatemod.sh` runs `go mod tidy` in every directory under `.` with a `go.mod`. That includes any local worktrees under `.claude/worktrees/`.

### Tests and what they need

`make test` needs nothing external.

Integration tests sit behind `//go:build integration` (well over a hundred files across the root package, `adapters/`, `forgeext/`, `migrations/`, `fabriqtest/` and `cmd/`). They start containers through `fabriqtest`: TimescaleDB with pgvector, plain Postgres for the catalog and the primary/standby harness, Redis, FalkorDB, Elasticsearch and ClickHouse. Without Docker they fail with `t.Fatalf`. Nothing skips quietly, and no env var points them at an existing database. The harness also provisions a non-superuser `fabriq_app` role, because RLS doesn't constrain superusers.

Other tags:

- `-tags duckdb` with `CGO_ENABLED=1` builds the embedded DuckDB analytics sink in `adapters/duckanalytics`. The default build stays CGO-free and must never need this tag.
- `-tags loadtest` runs the catalog load test, sized by `FABRIQ_LOADTEST_TENANTS`, `FABRIQ_LOADTEST_SECONDS`, `FABRIQ_LOADTEST_WORKERS` and `FABRIQ_LOADTEST_ADAPTIVE=1`.

`internal/relguard` skips unless HEAD sits exactly on a `vX.Y.Z` tag. On a release checkout it fails if the root's `fabriq/core` require doesn't equal that tag.

### What CI runs

`.github/workflows/ci.yml` starts with the shared `xraph/workflows/.github/workflows/go-ci.yml@v1`, which for the root runs `make test` (root and core), a race coverage run, golangci-lint, `gofmt -l .`, `make vet`, a `go mod tidy` diff on the root `go.mod` and `go.sum`, gosec (standalone, skipping only G115, so the `.golangci.yml` excludes don't apply) and govulncheck. Its nested-modules job rejects `v0.0.0` requires between the five modules.

After that finishes, these run in parallel:

| Job | What it does |
|---|---|
| core module | `go build ./...` and `go test -race -count=1 ./...` in `core/`, including `TestCoreHasNoDriverDependencies` |
| Integration | `make test-integration`, around 25 to 30 minutes, and it only starts once the shared job (about seven minutes) is done |
| Lint | One golangci-lint leg per module (golangci-lint `latest`, no `integration` tag) |
| Docker image | `make vendor`, then an amd64 build of `deploy/docker/Dockerfile`, pushed as `:edge` on main |
| DuckDB analytics (CGO) | The `duckdb` tests with CGO on |
| wardenauthz adapter | `go build` and `go test` in `adapters/wardenauthz` |
| Module version check | `./scripts/check-module-versions.sh` with full tag history |

`CI Success` only checks `ci`, `core`, `integration` and `docker`. Lint, DuckDB, wardenauthz and the module-version check can go red without failing it, so look at every job before you call main green. No job runs the tests in `remote/grpc` or `fabriqserver`. They only get linted, so run them yourself if you touch either.

## Layout

| Path | What lives there |
|---|---|
| `fabriq.go`, `open.go`, `config.go`, `options.go` | The `Fabriq` facade, `Open(ctx, reg, cfg, opts...)`, config, functional options |
| `errors.go` | Aliases of the core sentinels so callers write `fabriq.ErrX` |
| `fs_*.go`, `blob_*.go`, `catalogmode.go`, `sharding.go`, `docsync.go` | The file plane, blob write path, database-per-tenant catalog mode, shard routing, live fan-out of document updates |
| `core/` | The kernel module: `query` (ports and the `Fabric` interface), `registry`, `command`, `event`, `tenant`, `projection`, `subscribe`, `livequery`, `document`, `blob`, `cache`, `catalog`, `provision`, `sweep`, `analytics`, `insights`, `agent`, `crypto`, `fabriqerr`, `fabriqtest` |
| `adapters/` | Every engine dialect: `postgres`, `redis`, `falkordb`, `elastic`, `cache`, `shard`, `trove` (blob store), `pganalytics`, `chanalytics`, `duckanalytics`, `graphtest` (graph conformance), plus the `wardenauthz` module |
| `migrations/` | The DDL authority: grove Go-code migrations in group `fabriq`, `0001_outbox.go` onward |
| `forgeext/` | The Forge extension: DI wiring, worker, migrate step, gateway, `adminapi`, `agentmcp`, `shieldguard`, `gmmclusterer` |
| `remote/` | The remote `Fabric` client and handler over a codec-neutral transport, protobuf envelopes in `remote/fabriqpb` |
| `remote/grpc/`, `remote/grpc/cmd/fabriqserver/` | The gRPC binding module and the standalone server module |
| `cachequery/`, `conformance/`, `gateway/`, `client/` | Row cache decorator, cross-port conformance cases, the live-query edge tier, a DSN parser for remote clients |
| `domain/` | The example domain pack, and the only package allowed domain knowledge |
| `fabriqtest/` | Container harness and conformance shims for tests |
| `cmd/fabriq`, `cmd/api-example`, `cmd/admin-demo` | The worker and operator CLI, a demo API, a seeded demo backend for the fabriq-admin console |
| `deploy/docker`, `deploy/helm/fabriq` | Dockerfiles and the Helm chart |
| `docs/` | Fumadocs site, plus `OPERATIONS.md`, `MIGRATIONS.md` and ADRs in `docs/decisions/` |
| `internal/relguard`, `internal/metrics` | The release guard test, Prometheus instruments |

## Conventions

`fabriq.Open` takes a registry, a `Config` and functional options (`type Option func(*settings)`, such as `WithConflationWindow`). You declare each entity once, as an `EntitySpec` in a `registry.Registry`, and you write only through `f.Exec` with a `command.Command`. Never write to a projection engine directly.

Every call needs a tenant on the context (`tenant.WithTenant`), and without one you get `ErrNoTenant`. Only auth middleware should set it, and only from validated claims.

Errors are sentinels in `core/fabriqerr` and `core/tenant`, re-exported from the root. Rich types such as `VersionConflictError` and `NotFoundError` still match their sentinel through `Is`, so callers branch with `errors.Is`. Core and adapter code imports `fabriqerr`. Application code uses the `fabriq.` aliases.

Event IDs, and aggregate IDs the command plane generates, are ULIDs from `core/event.NewID()`. They're monotonic within a process, and the outbox relay depends on that ordering.

If you change the schema, you write a migration in `migrations/` by hand, because the registry never generates DDL. An integration test diffs `information_schema` against every registered spec in both directions. Ship changes expand, migrate, contract: no in-place renames or type changes, no `NOT NULL` without a default in one step. `docs/MIGRATIONS.md` has the rest.

The core and the root library do not log. They return errors, and `forgeext` logs through `forge.Logger`.

The Forge extension in `forgeext/` embeds `forge.BaseExtension`, reads config under `extensions.fabriq.*`, picks up a `*grove.DB` from the vessel container (named or default), and provides the facade (aliased `fabriq`, also as `query.Fabric`) and the registry (named `registry.ServiceName`) back to it.

### Lint rules that bite

- `depguard` keeps raw drivers in `adapters/`. Outside `adapters/`, `fabriqtest/` and `migrations/`, you can't import grove drivers, `grove/kv`, go-redis, go-elasticsearch, falkordb-go, pgx, trove or golang-lru. Go through a fabriq port.
- `depguard` also keeps `core/` pure, so it can't import `fabriq/domain`, `fabriq/adapters` or forge. `TestCoreHasNoDriverDependencies` adds a second fence on core's whole build graph (no ClickHouse, Elasticsearch, pgx, go-redis, trove, DuckDB, testcontainers, FalkorDB, orb or `k8s.io`).
- `govet` has every analyzer but `fieldalignment`. `shadow` is on in production code and off in tests.
- `errcheck` has `check-blank: false`, so `defer func() { _ = tx.Rollback() }()` is an accepted, deliberate discard.
- `revive`'s `exported` rule wants doc comments on exported names. `var-naming` is on here.
- Tests are exempt from gosec, errcheck, gocritic, noctx, depguard and unparam.

Commit subjects use conventional prefixes. goreleaser's changelog drops `docs:`, `test:`, `chore:` and `ci:` and groups `feat` and `fix`, so pick the prefix with that in mind.

## Dependencies

`core` requires only grove from the xraph set. The root requires `fabriq/core`, shield, the trove root module, grove (with pgdriver, `grove/kv` and its redisdriver), forge, confy and vessel. `adapters/wardenauthz` requires warden, forge and the root. `remote/grpc` requires the root, core and grove. Versions live in each `go.mod`.

Downstream, weave consumes `fabriq/core`. Cortex has two consumers: `integrations/fabriq` takes `fabriq/core`, and `inttest` takes the root.

To bump an xraph dependency:

```bash
GOWORK=off go get github.com/xraph/<mod>@vX.Y.Z   # in each module that requires it
./updatemod.sh                                    # tidy all five
```

Never commit a `go.work`, and never commit a `replace` that points at a sibling repo. The in-repo replaces stay.

## Releasing

Releases go through the dispatch workflow. Never push a tag by hand, even though the comment at the top of `release.yml` shows a `git tag` example.

```bash
gh workflow run release-dispatch.yml --ref main -f tag=vX.Y.Z -f dry_run=true    # rehearse
gh workflow run release-dispatch.yml --ref main -f tag=vX.Y.Z -f dry_run=false   # release
```

`dry_run` defaults to `true`, so a run without the flag only prints the plan.

Before you dispatch, every CI job on main has to be green, and every upstream xraph module fabriq needs (grove, shield, trove, forge, confy, warden) has to be released and pinned. Then check two require lines.

The root's `require github.com/xraph/fabriq/core` must still name the previous release. Leave it alone when you land changes. The dispatch bumps it itself, and if it's already at the new version the commit step has nothing to commit and fails, after `core/vX.Y.Z` is already pushed.

The other three modules aren't touched by the dispatch at all. Their requires on the root and on core (`remote/grpc`, `adapters/wardenauthz`, `fabriqserver`), and `fabriqserver`'s require on `remote/grpc`, change only when someone edits them in an ordinary commit. For v1.7.0 the release-prep commit pointed them at v1.7.0 before that tag existed. The replaces keep local builds working, and CI's nested-modules check reports an unpublished version as a notice without failing. Releases have also gone out with those lines left on the previous version. `fabriqserver`'s require on `remote/grpc` is the one people forget. If you bump any of them, use the version you're about to release.

With `dry_run=false`, the dispatch runs these steps in order:

1. Fails unless a PAT secret is set (`CUSTOM_GITHUB_TOKEN`, else `RELEASE_DISPATCH_TOKEN` or `GORELEASER_TOKEN`). A tag pushed with the default `GITHUB_TOKEN` wouldn't trigger `release.yml`.
2. Checks the tag is semver, the five `go` directives agree, and none of the five tags exist on origin.
3. Tags and pushes `core/vX.Y.Z`.
4. Runs `go mod edit -require=github.com/xraph/fabriq/core@vX.Y.Z` on the root.
5. Downloads `fabriq/core@vX.Y.Z` from the real proxy in a scratch module, with no replace in effect.
6. Commits `go.mod` and `go.sum` as `chore: bump fabriq/core requirement to vX.Y.Z` (github-actions[bot]) and pushes it to `main`.
7. Tags that commit `vX.Y.Z` and pushes it, which fires `release.yml`.
8. Tags the same commit `remote/grpc/vX.Y.Z`, `adapters/wardenauthz/vX.Y.Z` and `remote/grpc/cmd/fabriqserver/vX.Y.Z`.

So core's tag sits one commit before the other four. Don't push to `main` while it runs, or the push in step 6 is rejected with core already tagged.

`release.yml` then runs goreleaser: `fabriq` binaries for linux and darwin on amd64 and arm64, a multi-arch `ghcr.io/xraph/fabriq` image tagged with the version, major.minor and `latest`, and the GitHub release. A Helm job lints and packages `deploy/helm/fabriq` at the tag's version, pushes it to `oci://ghcr.io/xraph/charts` and attaches it to the release. A third job mirrors the image to Docker Hub (`vars.DOCKERHUB_REPO`, default `xraph/fabriq`) only when both `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` are set.

Never hand-tag `core/vX.Y.Z`. The dispatch refuses any version whose tags already exist, and the module-version check fails while core's latest tag is ahead of the root's. If a run dies partway, it prints which tags made it to origin. Don't delete and reuse them: cut the next patch.

After the release, weave and cortex re-pin.

## Branch rules

`main` has a ruleset that blocks deletion and force-pushes. There are no required checks or reviews, which is also why the bot's require-bump commit can push straight to `main`.
