<p align="center">
  <img src="assets/readme-banner.svg" alt="Fabriq, a Go data fabric that commits each write once in Postgres and projects it to FalkorDB, Elasticsearch, caches and live subscribers" width="100%">
</p>

# Fabriq: multi-tenant data fabric for Go with a transactional outbox

Fabriq is an open-source data fabric for Go backends that keep the same data in more than one engine. Every write is a command that commits once in a Postgres transaction, together with a versioned event in a transactional outbox, and a leader-elected relay fans that event out through Redis Streams to a FalkorDB knowledge graph, an Elasticsearch index, caches and live subscribers. Reads go through typed ports for relational, time-series (TimescaleDB), vector (pgvector), spatial (PostGIS), graph, full-text search and CRDT documents. Every call is tenant-scoped with Postgres row-level security, and the graph and search projections can always be rebuilt from Postgres. It runs as a library, as a [Forge](https://github.com/xraph/forge) extension, or as the `fabriq` worker binary.

[![Go 1.26+](https://img.shields.io/badge/Go-1.26%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/dl/) [![CI](https://github.com/xraph/fabriq/actions/workflows/ci.yml/badge.svg)](https://github.com/xraph/fabriq/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/xraph/fabriq.svg)](https://pkg.go.dev/github.com/xraph/fabriq) [![GitHub Stars](https://img.shields.io/github/stars/xraph/fabriq?style=flat)](https://github.com/xraph/fabriq/stargazers) [![Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

[Documentation](https://www.xraph.com/docs/fabriq) · [Quick start](#quick-start) · [How it works](#how-it-works) · [Features](#why-fabriq) · [Worker](#run-the-worker) · [Forge](#use-it-with-forge) · [Configuration](#configuration) · [FAQ](#faq)

## Quick start

You need [Go 1.26 or later](https://go.dev/dl/) and Docker. Fabriq's migrations enable TimescaleDB, pgvector and PostGIS, so start Postgres from an image that has all three, plus Redis:

```bash
docker run -d --name fabriq-pg -p 5432:5432 -e POSTGRES_PASSWORD=postgres timescale/timescaledb-ha:pg16-all
docker run -d --name fabriq-redis -p 6379:6379 redis:7-alpine
```

Create a module:

```bash
mkdir hello-fabriq
cd hello-fabriq
go mod init example.com/hello-fabriq
go get github.com/xraph/fabriq@latest
```

Save this as `main.go`. It declares one entity, applies Fabriq's migrations and its own table, then creates, updates and reads a note as tenant `acme`:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/migrations"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"
)

const dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

// Note is a fabriq entity. id, tenant_id and version are stamped by fabriq.
type Note struct {
	grove.BaseModel `grove:"table:notes"`

	ID       string `grove:"id,pk" json:"id"`
	TenantID string `grove:"tenant_id,notnull" json:"tenant_id"`
	Version  int64  `grove:"version,notnull" json:"version"`
	Title    string `grove:"title,notnull" json:"title"`
}

// The registry never generates DDL, so your tables come from your migrations.
var appMigrations = migrate.NewGroup("app", migrate.DependsOn(migrations.GroupName))

func init() {
	appMigrations.MustRegister(&migrate.Migration{
		Name:    "create_notes",
		Version: "20260101000001",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `CREATE TABLE IF NOT EXISTS notes (
				id TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				version BIGINT NOT NULL,
				title TEXT NOT NULL
			)`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `DROP TABLE notes`)
			return err
		},
	})
}

func main() {
	ctx := context.Background()

	// 1. Apply fabriq's migrations, then yours.
	pg := pgdriver.New()
	if err := pg.Open(ctx, dsn); err != nil {
		log.Fatal(err)
	}
	exec, err := migrate.NewExecutorFor(pg)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := migrate.NewOrchestrator(exec, migrations.Group(), appMigrations).Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	_ = pg.Close()

	// 2. Declare the entity and open the fabric.
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name:      "note",
		Kind:      registry.KindAggregate,
		Model:     (*Note)(nil),
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
	}); err != nil {
		log.Fatal(err)
	}

	f, _, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: dsn},
		Redis:    fabriq.RedisConfig{Addr: "localhost:6379"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// 3. Every call runs as a tenant.
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		log.Fatal(err)
	}

	res, err := f.Exec(tctx, command.Command{
		Entity:  "note",
		Op:      command.OpCreate,
		Payload: &Note{Title: "First note"},
	})
	if err != nil {
		log.Fatal(err)
	}

	expected := res.Version
	if _, err := f.Exec(tctx, command.Command{
		Entity:          "note",
		Op:              command.OpUpdate,
		AggID:           res.AggID,
		Payload:         &Note{Title: "First note, edited"},
		ExpectedVersion: &expected,
	}); err != nil {
		log.Fatal(err)
	}

	var n Note
	if err := f.Relational().Get(tctx, "note", res.AggID, &n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%q at version %d (event %s)\n", n.Title, n.Version, res.EventID)
}
```

Run it:

```bash
go mod tidy
go run .
# "First note, edited" at version 2 (event 01M4XW...)
```

Then look at the outbox:

```bash
docker exec fabriq-pg psql -U postgres -c "SELECT count(*) FROM fabriq_outbox"
```

You'll see two events, one per command, each written in the same transaction as the row it describes. Nothing has gone to Redis yet. Relaying the outbox, running the projections and feeding subscribers is the worker's job (see [Run the worker](#run-the-worker)).

Two things to know before you go further. `ExpectedVersion` makes the update fail with `fabriq.ErrVersionConflict` if someone else wrote first, so you never silently overwrite. And a context without a tenant gets `fabriq.ErrNoTenant` from every call. In a real service, only your auth middleware calls `tenant.WithTenant`, with a tenant ID taken from verified claims.

## How it works

```mermaid
flowchart TD
    CMD["commands: f.Exec(cmd), validated against the registry"]
    PG[("Postgres + TimescaleDB + pgvector + PostGIS<br/>row, versioned event and outbox commit together")]
    RELAY["leader-elected relay<br/>wakes on LISTEN/NOTIFY, publishes in order"]
    REDIS[["Redis Streams"]]
    GRAPH[("FalkorDB graph projection")]
    ES[("Elasticsearch search projection")]
    SUBS(["live deltas to subscribers"])
    CACHE["read-through cache, evicted by events"]
    PORTS["typed query ports"]

    CMD --> PG --> RELAY --> REDIS
    REDIS --> GRAPH
    REDIS --> ES
    REDIS --> SUBS
    REDIS -. evict .-> CACHE
    PORTS -. read .-> CACHE
    CACHE -. miss .-> PG
    CACHE -. miss .-> GRAPH
    CACHE -. miss .-> ES
```

You declare each entity once, as a `registry.EntitySpec`, and that one declaration tells Fabriq everything it needs to know about how your data should look in every engine: its Go model, whether it becomes a graph node or edge, which fields go to the search index, which scopes clients can subscribe to, and options for caching, embeddings and live queries. `f.Exec` and `f.ExecBatch` are your only write path. You never write to FalkorDB or Elasticsearch yourself. If one of them drifts or gets wiped, the reconciler or a blue-green rebuild brings it back from Postgres.

You can't forget the tenant. The tenant on your context becomes `SET LOCAL` plus row-level security in Postgres, a graph per tenant in FalkorDB, index routing in Elasticsearch and a key prefix in Redis, with a pre-query hook as a backstop. RLS doesn't constrain a Postgres superuser, so in production your app should connect as an ordinary role.

## Why Fabriq?

If you've kept Postgres, a search index and a graph in step before, you probably did it with a dual write: save the row, call Elasticsearch, and hope nothing fails in between. It works until a deploy, a timeout or a crashed pod lands between the two calls, and then you find out weeks later when someone searches for a record that the index never saw. With Fabriq the row and its event commit in one transaction, so either both exist or neither does, and everything downstream is rebuilt from that log.

| You need | Fabriq gives you |
| --- | --- |
| Writes that can't get lost on the way to other engines | Commands with optimistic concurrency and all-or-nothing batches, each committing one versioned event to a transactional outbox |
| Multi-tenant isolation you can't forget to apply | A tenant on every context, enforced through Postgres RLS and per-tenant graphs, indexes and keys |
| A knowledge graph | A FalkorDB projection queried with openCypher, with blue-green rebuilds |
| Full-text search | An Elasticsearch projection with per-tenant indexes and alias-swap rebuilds |
| Time-series, vector and geospatial queries | `Timeseries()` on TimescaleDB, `Vector()` on pgvector with HNSW, `Spatial()` on PostGIS |
| Live UIs | `Subscribe` for conflated, resumable deltas, and `LiveQuery` for a filtered, sorted window that sends enter, leave, move and update deltas |
| Collaborative documents | A CRDT document plane with an append-only update log, compaction and materialization into ordinary versioned events |
| Files | A file plane with folders, permissions, shares and bookmarks, and blobs stored through [Trove](https://github.com/xraph/trove) |
| Entities defined at runtime | Dynamic entities from a schema descriptor, with Fabriq-managed `CREATE` and additive `ALTER` |
| Room to grow | Sharding across Postgres databases, database-per-tenant or schema-per-tenant catalog mode, and a remote `Fabric` over gRPC |
| Answers when something drifts | A reconciler that finds missing, stale and zombie projection rows, `rebuild` and `reconcile` CLI commands, and Prometheus metrics |

The [architecture page](docs/content/docs/%28concepts%29/architecture.mdx) and [commands and events](docs/content/docs/%28concepts%29/commands-and-events.mdx) cover the write path in detail.

## Run the worker

`fabriq.Open` gives you the write path and the read ports. You still need a worker. The background work (the outbox relay, projection consumers, the reconciler, the document plane and blob GC) runs in a worker, and you can run it two ways.

Inside your own app, turn it on in the Forge extension with `forgeext.WithWorker(true)`, as in [Use it with Forge](#use-it-with-forge). For your own entities, that's the way to go.

Or run the `fabriq` binary, a Forge CLI app. With no arguments it runs `serve`, the worker, and it carries the operator commands too:

| Command | What it does |
| --- | --- |
| `fabriq serve` | Runs the worker, with Prometheus metrics at `/metrics` on `FABRIQ_HTTP_ADDR` (`:8081` by default) |
| `fabriq migrate up`, `down`, `status` | Runs Fabriq's migrations against `--dsn` or `FABRIQ_POSTGRES_DSN` |
| `fabriq rebuild` | Blue-green projection rebuild for a tenant |
| `fabriq reconcile` | Compares Postgres with the projections and re-emits drifted aggregates |
| `fabriq inspect registry`, `inspect state` | Dumps entity specs, or projection state for a tenant |
| `fabriq tenant ...`, `fabriq analytics ...` | Catalog-mode tenant provisioning, and analytics sink operations |

The binary reads `config.yaml` from the working directory or `/etc/fabriq`, and every key has a `FABRIQ_*` environment variable that wins over the file (`FABRIQ_POSTGRES_DSN`, `FABRIQ_REDIS_ADDR`, `FABRIQ_FALKORDB_ADDR`, `FABRIQ_ELASTICSEARCH_ADDRS`). It registers the example entities from [`domain/`](domain/). Releases ship it as binaries, as the `ghcr.io/xraph/fabriq` container image and as a Helm chart at `oci://ghcr.io/xraph/charts`. [`config.example.yaml`](config.example.yaml) documents every key, and [OPERATIONS.md](docs/OPERATIONS.md) has the runbooks.

## Use it with Forge

The `forgeext` package wraps Fabriq as a [Forge](https://github.com/xraph/forge) extension. It lives in the root module, so there's nothing extra to install.

```go
import (
	"github.com/xraph/forge"
	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/forgeext"
)

reg := registry.New()
reg.MustRegister(registry.EntitySpec{
	Name:      "note",
	Kind:      registry.KindAggregate,
	Model:     (*Note)(nil),
	Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
})

app := forge.NewApp(forge.AppConfig{
	Name:        "notes",
	HTTPAddress: ":8080",
	Extensions: []forge.Extension{
		forgeext.New(reg, forgeext.WithWorker(true)),
	},
})

err := app.Router().POST("/notes", func(ctx forge.Context) error {
	f, err := forge.Inject[*fabriq.Fabriq](app.Container())
	if err != nil {
		return err
	}

	// Your auth middleware stamps the tenant from verified claims.
	tctx, err := tenant.WithTenant(ctx.Context(), "acme")
	if err != nil {
		return err
	}

	res, err := f.Exec(tctx, command.Command{Entity: "note", Op: command.OpCreate, Payload: &Note{Title: "hello"}})
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusCreated, res)
})
```

The extension opens the fabric in `Start` and provides it three ways: as `*fabriq.Fabriq`, under the name `fabriq`, and as the `query.Fabric` interface from `core/query`. Resolve the interface when you can. Your package then doesn't import every adapter, and a deployment can put a remote `Fabric` behind the same key without your code changing. The registry is provided under the name `registry.ServiceName`.

If you don't configure a Postgres DSN, the extension borrows a `*grove.DB` from the container, so an app that already runs the [Grove](https://github.com/xraph/grove) extension on Postgres can share that connection. The extension also implements `forge.MigratableExtension`, so when you run your app through Forge's `cli.NewAppRunner`, its `migrate up`, `migrate down` and `migrate status` commands run Fabriq's migrations.

Next to it in `forgeext/` you'll find `NewGateway` (a live-query edge for SSE and WebSocket clients), `adminapi` (the HTTP API behind the [Fabriq Admin](https://github.com/xraph/fabriq-admin) console), `agentmcp` (the agent toolkit over MCP) and `shieldguard`.

### Configuration

The extension reads `extensions.fabriq` when you haven't passed a Postgres DSN or shards in code. The `fabriq` binary reads the same keys at the top level of its config file.

```yaml
extensions:
  fabriq:
    postgres:
      dsn: postgres://fabriq_app:secret@localhost:5432/fabriq?sslmode=disable
      pool_size: 16
    redis:
      addr: localhost:6379
    falkordb:
      addr: localhost:6380
    elasticsearch:
      addrs:
        - http://localhost:9200
```

| Key | What it does |
| --- | --- |
| `postgres.dsn`, `postgres.pool_size` | The source of truth. Required unless you set `shards`, use catalog mode, or let the extension borrow a `*grove.DB` |
| `shards`, `shardPins` | Several Postgres databases, with tenants placed by hash or pinned to a shard |
| `redis.addr`, `db`, `username`, `password` | Event fan-out, caches and subscriptions. Required when the worker runs |
| `falkordb.addr`, `username`, `password` | Turns on the graph projection |
| `elasticsearch.addrs`, `username`, `password` | Turns on the search projection |
| `subscriptions.conflation_window`, `stream_max_len`, `subscribe_buffer` | Delta plane tuning |
| `storage.storageDriver`, `defaultBucket`, `enableCas` | Blob storage for the file plane, as a [Trove](https://github.com/xraph/trove) DSN |
| `groveDatabase` | Which named `*grove.DB` to borrow when there's no DSN |

Catalog mode, analytics, insights, caching and encryption are fields on `fabriq.Config` that you set in code and pass with `forgeext.WithConfig`, because the YAML loader doesn't read them. Worker settings are options as well: `WithWorker`, `WithReconcileInterval`, `WithDocCompactInterval`, `WithBlobGCGrace` and `WithRollupInterval`, plus `WithEmbedder` and `WithSummarizer` for the embedding and distillation workers. The [configuration reference](docs/content/docs/reference/configuration.mdx) lists every field.

## Admin console

[Fabriq Admin](https://github.com/xraph/fabriq-admin) is a web console for a running fabric: entities, search, the graph, spatial and time-series queries, CRDT documents, files, caches and projections, with a tenant switcher. It talks to the `adminapi` extension under `/admin`.

[![Fabriq Admin console showing tenant-scoped entities and capabilities](.github/assets/admin-console.png)](https://github.com/xraph/fabriq-admin)

To try it from a clone of this repository, point `FABRIQ_POSTGRES_DSN` at your database and run `go run ./cmd/admin-demo`. It seeds demo data and serves the admin API on port 8080.

## Documentation

| Topic | Docs |
| --- | --- |
| Install and configure | [Installation](docs/content/docs/%28getting-started%29/installation.mdx), [configuration](docs/content/docs/%28getting-started%29/configuration.mdx) |
| Model your data | [Registry](docs/content/docs/%28concepts%29/registry.mdx), [dynamic entities](docs/content/docs/%28concepts%29/dynamic-entities.mdx), [tenancy](docs/content/docs/%28concepts%29/tenancy.mdx) |
| Keep engines in step | [Projections](docs/content/docs/%28concepts%29/projections.mdx), [rebuild and reconcile](docs/content/docs/%28operations%29/rebuild-and-reconcile.mdx) |
| Push changes to clients | [Subscriptions](docs/content/docs/%28concepts%29/subscriptions.mdx), [live queries](docs/content/docs/%28concepts%29/live-queries.mdx) |
| Query each engine | [Relational](docs/content/docs/%28data-planes%29/relational.mdx), [graph](docs/content/docs/%28data-planes%29/graph.mdx), [search](docs/content/docs/%28data-planes%29/search.mdx), [time series](docs/content/docs/%28data-planes%29/timeseries.mdx), [vector](docs/content/docs/%28data-planes%29/vector.mdx), [spatial](docs/content/docs/%28data-planes%29/spatial.mdx), [documents](docs/content/docs/%28data-planes%29/documents.mdx) |
| Scale out | [Sharding](docs/content/docs/%28concepts%29/sharding.mdx), [database per tenant](docs/content/docs/%28operations%29/db-per-tenant.mdx), [remote protocol](docs/content/docs/%28operations%29/remote-protocol.mdx) |
| Run it in production | [Deployment](docs/content/docs/%28operations%29/deployment.mdx), [migrations](docs/MIGRATIONS.md), [operations runbooks](docs/OPERATIONS.md) |

The rendered docs are at [xraph.com/docs/fabriq](https://www.xraph.com/docs/fabriq), and the Go API is on [pkg.go.dev](https://pkg.go.dev/github.com/xraph/fabriq).

## FAQ

### How do I keep Postgres and Elasticsearch in sync in Go?

Don't write to Elasticsearch from your request handler. With Fabriq you write a command, the row and an outbox event commit in one Postgres transaction, and the worker indexes the event. If the index falls behind or gets wiped, `fabriq reconcile` or a rebuild restores it from Postgres.

### Does Fabriq need FalkorDB and Elasticsearch?

No. Postgres is the only hard requirement, and you need Redis once the worker runs. The graph and search planes turn on when you give them an address, and until then `Graph()` and `Search()` return `fabriq.ErrStoreNotConfigured`.

### How does Fabriq handle multi-tenancy?

The tenant rides on `context.Context`, and every call requires one. Each engine applies it its own way: row-level security in Postgres, one graph per tenant in FalkorDB, routed indexes in Elasticsearch and prefixed keys in Redis. For harder isolation you can move to database-per-tenant or schema-per-tenant catalog mode.

### Does Fabriq generate my database schema?

No. The registry describes entities, and you write the DDL as migrations, the way the quick start does. An integration test in this repository compares the registry with `information_schema` to catch drift.

### Can I use Fabriq without Forge?

Yes. `fabriq.Open` works in any Go program. The `forgeext` package and the `fabriq` binary build on Forge, and if you only need entity specs and the port interfaces, you can depend on the lighter `github.com/xraph/fabriq/core` module alone.

## Contributing

Issues and focused pull requests are welcome on [GitHub](https://github.com/xraph/fabriq/issues). Use `GOWORK=off` so you get the module graph CI uses:

```bash
git clone https://github.com/xraph/fabriq.git
cd fabriq
GOWORK=off go build ./...
make test               # unit tests in the root and core/, no Docker
make test-integration   # Docker: TimescaleDB, Redis, FalkorDB, Elasticsearch, ClickHouse
make lint               # includes the depguard architecture boundaries
```

The integration tests start their own containers and fail (they don't skip) without Docker. `./...` doesn't cross into nested modules, so test `remote/grpc`, `remote/grpc/cmd/fabriqserver` and `adapters/wardenauthz` from their own directories. Schema changes are hand-written migrations under [`migrations/`](migrations/) that follow the expand, migrate, contract rules in [MIGRATIONS.md](docs/MIGRATIONS.md). [AGENTS.md](AGENTS.md) covers the module layout, lint boundaries and the release workflow. Commits follow [Conventional Commits](https://www.conventionalcommits.org/).

## License

Fabriq is available under the [Apache License 2.0](LICENSE).

Built by [Rex Raphael](https://github.com/juicycleff) and [contributors](https://github.com/xraph/fabriq/graphs/contributors), on [Grove](https://github.com/xraph/grove) and [Forge](https://github.com/xraph/forge).
