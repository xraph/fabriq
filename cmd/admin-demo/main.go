// Command admin-demo is a runnable demo server that mounts the fabriq adminapi
// extension against a live Postgres, so the fabriq-admin SPA can be exercised
// end-to-end against a real backend.
//
// It wires:
//
//   - a demo dynamic entity "product" (registry.DynamicSchema aggregate) plus
//     the adminapi plugin-remote schema (adminapi.PluginRemoteSpec);
//   - fabriq.Open (runs migrations) + EnsureDynamic for the dynamic tables;
//   - the fabriq forge extension (forgeext) and the adminapi extension, with
//     auth ON by default (auto-provisioned KeyStore + admin/admin login;
//     set ADMIN_DEMO_AUTH=0 to opt out to a keyless API with a tenant
//     middleware that resolves the tenant from the X-Tenant-ID header);
//   - an app-wide CORS middleware that allows the SPA origin and answers
//     preflight OPTIONS with 204;
//   - idempotent startup seeding of ~60 product rows per demo tenant so the
//     list endpoint exercises pagination (default page size 50).
//
// Environment:
//
//	FABRIQ_POSTGRES_DSN  Postgres DSN
//	                     (default postgres://fabriq:fabriq@localhost:5433/fabriq?sslmode=disable)
//	ADMIN_DEMO_ADDR      listen address (default ":8080")
//
// Run:
//
//	go run ./cmd/admin-demo
//
// Then:
//
//	curl -s localhost:8080/admin/meta -H 'X-Tenant-ID: acme-corp'
//	curl -s 'localhost:8080/admin/entities?type=product&limit=5' -H 'X-Tenant-ID: acme-corp'
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/forgeext/adminapi"
)

const (
	defaultDSN    = "postgres://fabriq:fabriq@localhost:5433/fabriq?sslmode=disable" //nolint:gosec // demo-only local DSN; not a real credential.
	defaultAddr   = ":8080"
	defaultES     = "http://localhost:9200"
	defaultFalkor = "localhost:6390"
	defaultRedis  = "localhost:6379"

	// productEntity is the demo dynamic entity type the SPA browses.
	productEntity = "product"

	// productSearchIndex is the logical search index base name for products.
	// The elastic adapter derives the per-tenant alias (fabriq_<tenant>_products)
	// and the versioned index behind it from this base.
	productSearchIndex = "products"

	// blobBucket is the default object-store bucket all file-plane bytes live
	// under (created idempotently by the trove adapter on Open).
	blobBucket = "fabriq-admin-demo"
)

// demoTenants is the fixed tenant set every seeder (products, customers,
// orders, places, graph, files, docs, distill, telemetry) iterates over.
var demoTenants = []string{"acme-corp", "globex"}

// demoAnalyticsSalt pseudonymizes the Hash fields of the customer/order analytics
// specs (email, customer_id). A real deployment keeps this secret and stable; the
// demo pins a fixed value so hashes are reproducible across restarts.
const demoAnalyticsSalt = "fabriq-admin-demo-analytics-salt-v1"

func main() {
	if err := run(); err != nil {
		log.Fatalf("admin-demo: %v", err)
	}
}

func run() error {
	ctx := context.Background()

	dsn := os.Getenv("FABRIQ_POSTGRES_DSN")
	if dsn == "" {
		dsn = defaultDSN
	}
	addr := os.Getenv("ADMIN_DEMO_ADDR")
	if addr == "" {
		addr = defaultAddr
	}
	esURL := os.Getenv("ELASTICSEARCH_URL")
	if esURL == "" {
		esURL = defaultES
	}
	falkorAddr := os.Getenv("FALKORDB_ADDR")
	if falkorAddr == "" {
		falkorAddr = defaultFalkor
	}
	// Redis backs the change-feed plane: it is what makes the live-query engine
	// (liveEngine in fabriq.Open) non-nil AND drives the outbox→redis relay (the
	// worker) that PUBLISHES deltas on every committed write. Without it, the
	// adminapi POST /admin/live endpoint degrades to 501. Override with REDIS_ADDR.
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = defaultRedis
	}

	// Blob/file-plane storage DSN. The fabriq blob adapter (adapters/trove)
	// drives a trove driver selected by the DSN scheme; trove ships file://,
	// local:// and mem:// drivers. There is NO S3/MinIO trove driver in the
	// pinned trove release, so the demo uses a persistent FILE-backed plane
	// (the dashboard browse/upload/download/delete flow is identical regardless
	// of the byte backend). Override with ADMIN_DEMO_BLOB_DSN to point at any
	// trove-supported DSN. Default: a file:// root under the OS temp dir so the
	// tree survives restarts within a boot.
	blobDSN := os.Getenv("ADMIN_DEMO_BLOB_DSN")
	if blobDSN == "" {
		blobDSN = "file://" + filepath.Join(os.TempDir(), "fabriq-admin-demo-blobs")
	}

	// Analytics: the columnar read model that backs the fabriq-admin Analytics
	// console (Freshness / Operations / Privacy). The DSN scheme selects the sink —
	// duckdb:// (the demo default under -tags duckdb), postgres:// or clickhouse://
	// via ADMIN_DEMO_ANALYTICS_DSN. dataDir holds the embedded-DuckDB file; an empty
	// DSN (untagged build with no override) leaves analytics disabled and the demo
	// still runs. See analytics_duckdb.go / analytics_noduckdb.go for the seam.
	dataDir := os.Getenv("ADMIN_DEMO_DATA_DIR")
	if dataDir == "" {
		dataDir = filepath.Join(os.TempDir(), "fabriq-admin-demo")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	analyticsDSN := os.Getenv("ADMIN_DEMO_ANALYTICS_DSN")
	if analyticsDSN == "" {
		analyticsDSN = defaultAnalyticsDSN(dataDir)
	}

	// The demo embedder is deterministic and NON-semantic (see embedder.go). It
	// is used both to embed the seeded rows into pgvector and, via
	// adminapi.WithEmbedder, to embed free-text vector queries at request time so
	// the same illustrative space is searched on both sides.
	embedder := newLocalEmbedder()

	// 1. Build the registry: the demo dynamic "product" entity + the adminapi
	//    plugin-remote schema. Both are DynamicSchema aggregates, written and
	//    read map-natively by the command plane and the relational querier.
	reg := registry.New()
	if err := reg.Register(productSpec()); err != nil {
		return err
	}
	// The richer demo dynamic entities: customer + order. Both are DynamicSchema
	// aggregates like product, search-indexed, and seeded/indexed/graph-wired by
	// the same direct write paths in the tenant loop below.
	if err := reg.Register(customerSpec()); err != nil {
		return err
	}
	if err := reg.Register(orderSpec()); err != nil {
		return err
	}
	// The demo dynamic "place" entity carries geometry: each row is relational
	// (name/category/city) AND a point in the spatial plane (fabriq_geometries),
	// seeded via Spatial().Upsert below so the SPA can run radius queries.
	if err := reg.Register(placeSpec()); err != nil {
		return err
	}
	// The demo dynamic "site" + "equipment" entities: an asset-anchored,
	// tag-filtered spatial scenario layered on top of place. Each site is a
	// facility anchor and each equipment row is a tagged asset ringing it;
	// both are relational records AND points in the spatial plane, seeded via
	// Spatial().Upsert below (see equipment_seed.go).
	if err := reg.Register(siteSpec()); err != nil {
		return err
	}
	if err := reg.Register(equipmentSpec()); err != nil {
		return err
	}
	if err := reg.Register(adminapi.PluginRemoteSpec()); err != nil {
		return err
	}
	// Register the demo KindDocument entity "page" so (a) the registry-derived
	// crdt capability flips to true and (b) the postgres document store accepts
	// "page/<id>" document ids. Its physical table is created via EnsureDynamic
	// below; the CRDT update/snapshot tables come from fabriq's migrations.
	if err := reg.Register(pageSpec()); err != nil {
		return err
	}
	// Register a second KindDocument entity "note" so the Documents viewer has
	// more than one doc type (page + note) to browse.
	if err := reg.Register(noteSpec()); err != nil {
		return err
	}
	// Register the file-plane entities (blob_object + fs_node) so the command
	// executor knows their shape; their physical tables come from fabriq's
	// migrations (fabriq_fs_nodes / fabriq_blob_objects / fabriq_blob_cas), already present in the
	// demo Postgres.
	for _, spec := range fileSeedSpecs() {
		if err := reg.Register(spec); err != nil {
			return err
		}
	}
	// Register the typed digest_node entity so (a) the context-distillation tree
	// can be built and read (the Distiller writes nodes; the agent Toolkit's
	// Map/Digest read them) and (b) the adminapi distill capability flips to true.
	// Its physical table (fabriq_digest_nodes) comes from fabriq's migrations (0022-0024),
	// run by fabriq.Open — no EnsureDynamic needed (it is a typed grove model).
	if err := reg.Register(digestNodeSpec()); err != nil {
		return err
	}
	if err := reg.Validate(); err != nil {
		return err
	}

	// 2. Open fabriq with a bare Postgres source of truth. Open dials the shard,
	//    assembles the facade AND runs fabriq's migrations (the command store,
	//    relational ports and projection bookkeeping tables). The Config shape
	//    mirrors cmd/api-example and cmd/fabriq: Postgres{DSN}.
	// Elasticsearch.Addrs alone configures the search READ port (fabriq.Open
	// wires elastic.Open whenever Addrs is non-empty); it does NOT require Redis
	// or Projections.Search, which only gate the async projection WORKER. The
	// demo writes the search index directly in the seed step, so the worker is
	// not enabled. Security is disabled on the dev cluster, so no Username/Password.
	// FalkorDB.Addr alone configures the graph READ/WRITE QUERIER (fabriq.Open
	// wires falkordb.Open whenever Addr is non-empty); it does NOT require Redis
	// or Projections.Graph, which only gate the async projection WORKER. The demo
	// writes graph nodes/edges directly in the seed step (Graph().ApplyMutations),
	// so the worker is not enabled. With only Addr set, the live-target resolver
	// finds no projection_state row and falls back to registry.GraphName(tenant)
	// (tenant_<id>), so reads and direct writes target the same per-tenant graph.
	// Storage configures the blob/file byte plane. StorageDriver is a trove DSN
	// (file:// here — see blobDSN above) and DefaultBucket is the bucket all keys
	// live under (created idempotently by the adapter on Open). EnableCas wires
	// the content-addressable store the file-plane write path (CreateFile →
	// PutBlob) and read path (GetBlob) require; it is backed by the fabriq_blob_cas
	// ledger in the primary Postgres shard. With both set, f.Blob() is wired so
	// the adminapi capability probe reports files:true and the file endpoints
	// serve real bytes.
	cfg := fabriq.Config{
		Postgres:      fabriq.PostgresConfig{DSN: dsn},
		Elasticsearch: fabriq.ElasticsearchConfig{Addrs: []string{esURL}},
		FalkorDB:      fabriq.FalkorDBConfig{Addr: falkorAddr},
		// Redis wires the change-feed plane: the live-query engine (fabriq.Open
		// builds liveEngine only when Redis + the relational oracle are present)
		// and the by-tenant delta channel the adminapi POST /admin/live endpoint
		// tails. The outbox→redis relay that PUBLISHES those deltas runs in the
		// forgeext worker (enabled via WithWorker below).
		Redis: fabriq.RedisConfig{Addr: redisAddr},
		Storage: fabriq.StorageConfig{
			StorageDriver: blobDSN,
			DefaultBucket: blobBucket,
			EnableCas:     true,
		},
		// Analytics wires the opt-in columnar sink + proj:analytics consumer. An
		// empty DSN disables it, so the untagged build (analyticsDSN == "") simply
		// runs without analytics. HashSalt is set because the customer/order specs
		// mark Hash fields (email, customer_id) — required by Validate when enabled.
		Analytics: fabriq.AnalyticsConfig{DSN: analyticsDSN, HashSalt: demoAnalyticsSalt},
	}
	f, stores, err := fabriq.Open(ctx, reg, cfg)
	if err != nil {
		return err
	}

	// 3. EnsureDynamic creates the physical Postgres table for each dynamic
	//    entity (managed additive DDL). fabriq.Open exposes the primary shard's
	//    *postgres.Adapter via Stores.Postgres, which carries EnsureDynamic.
	for _, name := range []string{productEntity, customerEntity, orderEntity, placeEntity, siteEntity, equipmentEntity, adminapi.PluginRemoteEntityType, pageEntity, noteEntity} {
		ent, ok := reg.Get(name)
		if !ok {
			_ = f.Close()
			return missingEntityError(name)
		}
		if err := stores.Postgres.EnsureDynamic(ctx, ent); err != nil {
			_ = f.Close()
			return err
		}
	}

	// 4. Idempotent seeding: ensure each demo tenant has a populated product
	//    catalogue (~60 rows) so the SPA's list endpoint pages. Uses the real
	//    fabric's command executor under a tenant-stamped context.
	indexedTotal := 0
	graphNodesTotal, graphEdgesTotal := 0, 0
	fsFoldersTotal, fsFilesTotal := 0, 0
	docsSeeded := 0
	placesTotal := 0
	equipmentTotal := 0
	distillNodesTotal := 0
	telemetryPointsTotal := 0
	for _, tid := range demoTenants {
		if err := seedProducts(ctx, f, tid, seedCount); err != nil {
			_ = f.Close()
			return err
		}
		// Customers depend only on the tenant; orders reference real seeded
		// customer + product ids, so they must seed AFTER both products and
		// customers exist. Both mirror seedProducts (count-guarded, command
		// executor under tenant context).
		if err := seedCustomers(ctx, f, tid, customerCount); err != nil {
			_ = f.Close()
			return err
		}
		if err := seedOrders(ctx, f, tid, orderCount); err != nil {
			_ = f.Close()
			return err
		}
		// Seed the place entity + its geometry into the spatial plane
		// (fabriq_geometries via Spatial().Upsert, SRID 4326). Count-guarded, so
		// re-running on every startup is safe. Each tenant uses DIFFERENT cities
		// (acme-corp: SF + NYC; globex: London/Berlin/Tokyo) so tenant isolation
		// is visible in a radius query.
		pn, perr := seedPlaces(ctx, f, tid)
		if perr != nil {
			_ = f.Close()
			return perr
		}
		placesTotal += pn
		// Seed the site + equipment entities + their geometry into the spatial
		// plane, layered on top of place: one site per tenant (near that
		// tenant's place cluster) ringed by 8 tagged equipment points. This is
		// what exercises the adminapi spatial endpoint's centerId (near-asset)
		// and tag filter wiring against live demo data. Count-guarded on the
		// equipment table, so re-running on every startup is safe.
		en, eerr := seedEquipment(ctx, f, tid)
		if eerr != nil {
			_ = f.Close()
			return eerr
		}
		equipmentTotal += en
		// Populate the Search (Elasticsearch) and Vector (pgvector) planes for the
		// tenant's products via the DIRECT write paths (Search.ApplyMutations and
		// Vector.Upsert) — no projection worker. Idempotent: ES bulk gates on the
		// row version (external_gte) and vector upsert is ON CONFLICT DO UPDATE, so
		// re-running on every startup re-indexes harmlessly.
		n, ierr := indexProducts(ctx, f, embedder, tid)
		if ierr != nil {
			_ = f.Close()
			return ierr
		}
		indexedTotal += n
		// Index customers + orders into Search + Vector the same way, so
		// /search?type=customer and semantic queries work for the new types too.
		cn, cierr := indexCustomers(ctx, f, embedder, tid)
		if cierr != nil {
			_ = f.Close()
			return cierr
		}
		indexedTotal += cn
		on, oierr := indexOrders(ctx, f, embedder, tid)
		if oierr != nil {
			_ = f.Close()
			return oierr
		}
		indexedTotal += on

		// Populate the knowledge graph (FalkorDB) for the tenant's products via the
		// DIRECT write path (Graph().ApplyMutations with target "" → the tenant's
		// live graph tenant_<id>). It upserts a Category node per category and each
		// product as a Product node, then links products to categories
		// (IN_CATEGORY) and a few same-category products to one another (RELATED_TO).
		// Idempotent: the falkordb dialect renders every upsert as a version-gated
		// MERGE, so re-running on every startup is safe.
		gn, ge, gerr := seedGraph(ctx, f, tid)
		if gerr != nil {
			_ = f.Close()
			return gerr
		}
		graphNodesTotal += gn
		graphEdgesTotal += ge

		// Add the e-commerce cross-type layer on top of the product/category
		// graph: Customer + Order + Country nodes and PLACED / CONTAINS / LIVES_IN
		// edges. CONTAINS points at the same Product nodes seedGraph created, so
		// the two layers are connected and traverse/neighbors from a customer
		// reach products (and their categories). Same direct, version-gated MERGE
		// write path, so re-running on every startup is safe.
		egn, ege, egerr := seedEcommerceGraph(ctx, f, tid)
		if egerr != nil {
			_ = f.Close()
			return egerr
		}
		graphNodesTotal += egn
		graphEdgesTotal += ege

		// Seed a small file tree (folders + small text files) into the blob/CAS
		// plane via the file-plane facade (CreateFolder/CreateFile). Idempotent:
		// it skips when the tenant's root already carries the seed folders.
		ff, fl, ferr := seedFileTree(ctx, f, tid)
		if ferr != nil {
			_ = f.Close()
			return ferr
		}
		fsFoldersTotal += ff
		fsFilesTotal += fl

		// Add a few more real-content files (docs/changelog.md, docs/api-notes.txt,
		// images/diagram.txt) under the folders seedFileTree created. Per-file
		// existence check, so it composes with the original seed and is idempotent.
		ef, eferr := seedExtraFiles(ctx, f, tid)
		if eferr != nil {
			_ = f.Close()
			return eferr
		}
		fsFilesTotal += ef

		// Seed one demo CRDT document ("page/welcome") into the document plane
		// via the DIRECT document-store write path (Document().ApplyUpdate with
		// LWW change records), then verify it merges via Snapshot. Idempotent: it
		// skips when the doc's merged state already carries the seeded title. This
		// is what makes the adminapi crdt endpoints serve real data and the
		// capability probe report crdt:true (the "page" KindDocument entity is
		// registered above).
		ok, derr := seedDemoDoc(ctx, f, tid)
		if derr != nil {
			_ = f.Close()
			return derr
		}
		if ok {
			docsSeeded++
		}

		// Seed two more CRDT docs (page/about + note/roadmap) so the Documents
		// viewer has multiple docs across two doc types. Same direct ApplyUpdate
		// write path, Snapshot-probed for idempotency.
		extra, exderr := seedExtraCRDTDocs(ctx, f, tid)
		if exderr != nil {
			_ = f.Close()
			return exderr
		}
		docsSeeded += extra

		// Build the context-distillation Merkle tree from the seeded product +
		// customer rows: L0 leaves per row, L1 scope nodes (per product status /
		// customer tier), and the L2 tenant root. Uses the demo embedder + a
		// DETERMINISTIC stub summarizer (admin-demo has NO LLM — see
		// distill_seed.go) + the CAS. Idempotent: skipped when the tenant root
		// already exists. This is what makes /admin/distill/map serve a real tree.
		built, drep, dderr := seedDistillTree(ctx, f, reg, embedder, stores.CAS, tid)
		if dderr != nil {
			_ = f.Close()
			return dderr
		}
		if built {
			distillNodesTotal += drep.Built
		}

		// Seed a day of deterministic telemetry (cpu/mem/requests/latency signals)
		// into the timeseries plane via the DIRECT bulk-write path
		// (Timeseries().BulkWrite → tag_readings). Phase-shifted per tenant so the
		// same key reads differently across tenants. Presence-guarded, so
		// re-running on every startup does not duplicate points. This is what makes
		// the adminapi timeseries endpoints serve real data and the capability
		// probe report timeseries:true.
		tp, tperr := seedTelemetry(ctx, f, tid)
		if tperr != nil {
			_ = f.Close()
			return tperr
		}
		telemetryPointsTotal += tp
	}

	// Populate the analytics read model from the just-seeded aggregates so the
	// Analytics console has facts immediately, rather than only after the live
	// proj:analytics consumer (in the serving worker) catches up. Backfill is
	// idempotent (version-gated upserts), so re-running on every boot is a no-op.
	// Only runs when analytics is configured; skipped for the untagged/off build.
	analyticsFactsTotal := 0
	if analyticsDSN != "" {
		bf, bferr := stores.AnalyticsBackfiller(reg)
		if bferr != nil {
			_ = f.Close()
			return bferr
		}
		counts, bferr := bf.AllTenants(ctx, demoTenants, 2)
		if bferr != nil {
			_ = f.Close()
			return bferr
		}
		for _, n := range counts {
			analyticsFactsTotal += n
		}
		log.Printf("  analytics: backfilled %d fact(s) across %d tenant(s) into %s", analyticsFactsTotal, len(demoTenants), analyticsDSN)
	} else {
		log.Printf("  analytics: DISABLED (set ADMIN_DEMO_ANALYTICS_DSN=postgres://… or rebuild with -tags duckdb for embedded DuckDB)")
	}

	// The prep fabric has done its job (migrations, DDL, seeding, analytics
	// backfill). Closing it releases the analytics sink's handle too (e.g. the
	// embedded-DuckDB file lock) BEFORE the serving fabric — opened independently
	// by the forgeext extension at app.Start — reopens the same DSN.
	if err := f.Close(); err != nil {
		return err
	}

	// 5. Build the forge app and register the extensions. forgeext.Start opens
	//    its OWN fabriq facade (same DSN + Redis) and adminapi resolves that
	//    facade as its parent. The worker is ENABLED (WithWorker below) so the
	//    outbox→redis relay publishes deltas that drive the live-query SSE plane.
	app := forge.NewApp(forge.AppConfig{
		Name:        "fabriq-admin-demo",
		Version:     "0.1.0",
		HTTPAddress: addr,
	})

	// App-wide CORS so the SPA (any localhost dev port) can call the admin API
	// and its preflight OPTIONS gets a 204 short-circuit. UseGlobal runs before
	// routing, so the preflight never needs a registered OPTIONS route.
	app.Router().UseGlobal(corsMiddleware)

	// WithWorker enables the leader-elected background worker. Its outbox relay
	// (postgres.NewRelay) is the PRODUCER side of the live-query change feed: it
	// tails each shard's command outbox and republishes committed envelopes onto
	// the by-tenant Redis channel the live engine consumes. Without it, writes
	// commit but no delta ever reaches an open /admin/live SSE stream. The worker
	// requires both Redis and Postgres (both up on the demo stack).
	fabricExt := forgeext.New(reg, forgeext.WithConfig(cfg), forgeext.WithWorker(true))
	if err := app.RegisterExtension(fabricExt); err != nil {
		return err
	}

	adminOpts := []adminapi.Option{
		// The demo embedder backs TEXT-mode vector queries (POST /search/vector
		// with {query}). Similar-to-entity ({id}) reuses a stored embedding and
		// needs no embedder. Both query the same illustrative space the seed step
		// indexed rows into.
		adminapi.WithEmbedder(embedder),
		// Guarded-write allowlist for POST /agent/remember (deny-by-default). The
		// demo permits product creates/updates so the Remember surface has
		// something allowed to exercise; every other entity/op stays denied (403).
		adminapi.WithWritePolicy(agent.WritePolicy{
			Allow: map[string][]command.Op{
				productEntity: {command.OpCreate, command.OpUpdate},
			},
		}),
		// Enable the privileged schema-ops surface (run/rollback migrations,
		// ad-hoc DDL) so the demo exercises the Migrations console end to end.
		adminapi.WithSchemaAdmin(),
		// Analytics console: Read gates the Freshness/status dashboard; Admin
		// unlocks the Operations (backfill/reconcile) and Privacy (reproject/purge)
		// actions. Backed by the analytics sink configured in cfg.Analytics above.
		adminapi.WithAnalyticsRead(),
		adminapi.WithAnalyticsAdmin(),
		// Connection-pool observability (GET /admin/connections) and tenant
		// management, so those console surfaces are live against the demo too.
		adminapi.WithConnectionsRead(),
		adminapi.WithTenantsAdmin(),
	}
	if os.Getenv("ADMIN_DEMO_AUTH") == "0" {
		// Opt out: keyless admin API. The extension is auth-agnostic when
		// disabled, so attach tenant resolution ourselves (X-Tenant-ID).
		adminOpts = append(adminOpts,
			adminapi.WithAuthDisabled(),
			adminapi.WithRouteOptions(forge.WithMiddleware(tenantMiddleware)),
		)
		log.Printf("admin-demo: ADMIN_DEMO_AUTH=0 — auth DISABLED (keyless); X-Tenant-ID selects tenant")
	} else {
		// Auth ON by default: the library auto-provisions the KeyStore and a
		// default admin/admin login. The auth middleware resolves tenant (from
		// the key, or X-Tenant-ID for the multi-tenant session), so NO manual
		// tenantMiddleware here. Override creds via ADMIN_LOGIN_USER/PASSWORD.
		if pass := os.Getenv("ADMIN_LOGIN_PASSWORD"); pass != "" {
			user := os.Getenv("ADMIN_LOGIN_USER")
			if user == "" {
				user = "admin"
			}
			adminOpts = append(adminOpts, adminapi.WithAdminLogin(user, pass))
			log.Printf("admin-demo: dashboard login enabled for user %q (override)", user)
		}
		log.Printf("admin-demo: auth ENABLED by default — log in with admin/admin (unless ADMIN_LOGIN_* set); set ADMIN_DEMO_AUTH=0 to disable")
	}
	adminExt := adminapi.NewAdminAPI(fabricExt, adminOpts...)
	if err := app.RegisterExtension(adminExt); err != nil {
		return err
	}

	logStartup(addr, esURL, falkorAddr, blobDSN, embedder.Dims(), indexedTotal, graphNodesTotal, graphEdgesTotal, fsFoldersTotal, fsFilesTotal, docsSeeded, placesTotal, equipmentTotal, distillNodesTotal, telemetryPointsTotal)
	return app.Run()
}

// tenantMiddleware reads X-Tenant-ID from the request header and stamps the
// tenant into the request context, mirroring the host auth middleware adminapi
// requires in production (see forgeext/adminapi/http_test.go tenantMiddleware).
// Requests without the header are rejected with 400 so the contract is explicit.
func tenantMiddleware(next forge.Handler) forge.Handler {
	return func(ctx forge.Context) error {
		tid := ctx.Request().Header.Get("X-Tenant-ID")
		if tid == "" {
			return forge.BadRequest("missing X-Tenant-ID header")
		}
		tctx, err := tenant.WithTenant(ctx.Request().Context(), tid)
		if err != nil {
			return forge.BadRequest("invalid tenant id: " + err.Error())
		}
		// WithContext mutates the forge context in place (replaces the request's
		// context), so downstream handlers and the fabric see the tenant.
		ctx.WithContext(tctx)
		return next(ctx)
	}
}

// corsMiddleware is a small app-wide CORS middleware tuned for the fabriq-admin
// SPA: it allows any origin (no credentials), the admin verb set, and the
// Authorization / X-Tenant-ID / Content-Type request headers, and short-circuits
// preflight OPTIONS with 204. A custom middleware (rather than middleware.CORS) is
// used so a bare OPTIONS preflight — without an Access-Control-Request-Method
// header — still returns 204 rather than falling through to routing.
func corsMiddleware(next forge.Handler) forge.Handler {
	return func(ctx forge.Context) error {
		w := ctx.Response()
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		// Authorization is required now that auth is on by default — the SPA
		// sends `Authorization: Bearer <session token>`; without it here the
		// browser's preflight rejects every authed request.
		h.Set("Access-Control-Allow-Headers", "Authorization,Content-Type,X-Tenant-ID")
		// Expose Content-Disposition so the SPA's file download can read the
		// server-provided attachment filename cross-origin (browsers hide
		// non-safelisted response headers from fetch() unless exposed).
		h.Set("Access-Control-Expose-Headers", "Content-Disposition")

		if ctx.Request().Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
		return next(ctx)
	}
}

// productSpec returns the demo dynamic entity spec for "product": a few simple
// columns the SPA's entity browser can render and page through.
func productSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: productEntity,
		Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_products",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "sku", Type: registry.ColText, NotNull: true},
				{Name: "price", Type: registry.ColFloat},
				{Name: "status", Type: registry.ColText},
			},
		},
		// Search marks the product entity as search-indexed: Index is the logical
		// base name (tenant routing derived) and Fields are the columns included in
		// the indexed document and validated as multi_match / filter targets. This
		// is what makes fab.Search() accept "product" and the per-type capability
		// probe report search:true for the type.
		// Analytics opts product into the columnar analytics sink: name/sku/price/
		// status cross the trust boundary (no PII here, so no Hash). Deny-by-default
		// means only these listed fields are projected — the Analytics console's
		// Freshness/Operations views read the resulting facts.
		Analytics: &registry.AnalyticsSpec{
			Include: []string{"name", "sku", "price", "status"},
		},
		Search: registry.SearchSpec{
			Index:  productSearchIndex,
			Fields: []string{"name", "sku", "status"},
		},
		// Cache opts the product entity into the read-through row cache (P3). Reads
		// of product rows are served from the engine cache (backed by the demo's
		// Redis) and invalidated per-entity on write. This is what makes the
		// adminapi /admin/cache endpoint list a real keyspace ("product:q") and the
		// invalidate control flush something. TTL bounds each cached row.
		Cache: &registry.CacheSpec{TTL: 5 * time.Minute},
		// Subscribe declares the change channels the outbox relay fans committed
		// events out to. ByTenant is REQUIRED for the live-query plane: the live
		// engine's feed tails changes:{tenant}:tenant:{tenant}, so without ByTenant
		// here no product write ever reaches an open /admin/live stream. ByID also
		// powers single-aggregate subscriptions.
		Subscribe: []registry.Scope{registry.ByTenant, registry.ByID},
		// Live opts the product entity into the maintained-result-set live query
		// engine (adminapi POST /admin/live). Filterable/Sortable allowlist the
		// columns the SPA may filter/sort a live window on; MaxWindow caps the
		// page size. Requires Redis to be configured (the change feed) — see the
		// fabriq.Config.Redis wiring in run().
		Live: &registry.LiveSpec{
			Filterable: []string{"name", "sku", "status", "price"},
			Sortable:   []string{"name", "sku", "price"},
			MaxWindow:  500,
		},
		// Distill opts the product entity into context distillation: each row's
		// name+sku+status forms the L0 source text, and "status" is the scope name
		// that groups products into L1 scope-backbone digest nodes (one per status
		// value). This is what makes the seed step build a real digest tree the
		// adminapi /admin/distill/map endpoint serves. The summarizer is supplied by
		// the demo (a deterministic stub — admin-demo has no LLM); see
		// distill_seed.go.
		Distill: &registry.DistillSpec{
			SourceFields: []string{"name", "sku", "status"},
			Scopes:       []string{"status"},
		},
	}
}

// missingEntityError reports a registry lookup miss for a just-registered entity.
type missingEntityError string

func (e missingEntityError) Error() string {
	return "admin-demo: entity " + string(e) + " not found in registry after registration"
}

// logStartup prints the base URL, what got wired (ES url, FalkorDB addr,
// embedder dims, indexed products, seeded graph nodes/edges), and a couple of
// sample curl commands.
func logStartup(addr, esURL, falkorAddr, blobDSN string, dims, indexed, graphNodes, graphEdges, fsFolders, fsFiles, docs, places, equipment, distillNodes, telemetryPoints int) {
	base := "http://localhost" + addr
	log.Printf("admin-demo listening on %s (admin base: %s/admin)", addr, base)
	log.Printf("  seeded tenants: acme-corp, globex (%d products each)", seedCount)
	log.Printf("  search: elasticsearch=%s (index base %q)", esURL, productSearchIndex)
	log.Printf("  vector: pgvector via local deterministic embedder (dims=%d)", dims)
	log.Printf("  indexed %d products into search + vector across tenants", indexed)
	log.Printf("  graph: falkordb=%s wired; seeded %d nodes + %d edges across tenants (per-tenant graph tenant_<id>)", falkorAddr, graphNodes, graphEdges)
	log.Printf("  files: blob=%s bucket=%q CAS=on; seeded %d folders + %d files across tenants", blobDSN, blobBucket, fsFolders, fsFiles)
	log.Printf("  crdt: document plane (postgres + grove-crdt) wired; seeded %d demo doc(s) (%s/%s) across tenants", docs, pageEntity, demoPageID)
	log.Printf("  spatial: postgis (fabriq_geometries) wired; seeded %d place point(s) across tenants (acme-corp: SF+NYC, globex: London/Berlin/Tokyo)", places)
	log.Printf("  spatial: place (category) + site/equipment (tag) — try near-asset + tag filter in the Spatial page; seeded %d equipment point(s) across tenants", equipment)
	log.Printf("  distill: context-distillation tree (digest_node) built from product+customer rows via demo embedder + stub summarizer + CAS; %d L0 leaves across tenants", distillNodes)
	log.Printf("  timeseries: tag_readings (plain-PG telemetry table) wired; bulk-wrote %d readings across tenants (signals: cpu.load, mem.used.pct, requests.rate, latency.p95.ms)", telemetryPoints)
	log.Printf("  try: curl -s %s/admin/capabilities -H 'X-Tenant-ID: acme-corp'  # search:true vector:true graph:true files:true crdt:true distill:true", base)
	log.Printf("  try: curl -s '%s/admin/search?type=product&q=Product&limit=5' -H 'X-Tenant-ID: acme-corp'", base)
	log.Printf("  try: curl -s -X POST %s/admin/search/vector -H 'Content-Type: application/json' -H 'X-Tenant-ID: acme-corp' -d '{\"type\":\"product\",\"query\":\"widget\",\"k\":5}'", base)
	log.Printf("  try: curl -s '%s/admin/graph/neighbors?type=product&id=<id>&limit=10' -H 'X-Tenant-ID: acme-corp'", base)
	log.Printf("  try: curl -s '%s/admin/files' -H 'X-Tenant-ID: acme-corp'  # root folders/files", base)
	log.Printf("  try: curl -s '%s/admin/crdt/%s/%s' -H 'X-Tenant-ID: acme-corp'  # merged CRDT snapshot", base, pageEntity, demoPageID)
	log.Printf("  try: curl -s '%s/admin/crdt/%s/%s/updates' -H 'X-Tenant-ID: acme-corp'  # update-log metadata", base, pageEntity, demoPageID)
	log.Printf("  try: curl -s '%s/admin/entities?type=product&limit=5' -H 'X-Tenant-ID: acme-corp'", base)
	log.Printf("  try: curl -s -X POST %s/admin/spatial/within -H 'Content-Type: application/json' -H 'X-Tenant-ID: acme-corp' -d '{\"entity\":\"place\",\"lng\":-122.42,\"lat\":37.77,\"radiusM\":50000,\"limit\":10}'  # places near SF", base)
	log.Printf("  try: curl -s -X POST %s/admin/spatial/within -H 'Content-Type: application/json' -H 'X-Tenant-ID: acme-corp' -d '{\"entity\":\"equipment\",\"centerEntity\":\"site\",\"centerId\":\"<plant-a-id>\",\"radiusM\":500,\"filter\":{\"tag\":\"pump\"},\"limit\":10}'  # pumps near plant-a", base)
	log.Printf("  try: curl -s '%s/admin/distill/map' -H 'X-Tenant-ID: acme-corp'  # context-distillation tree outline", base)
	log.Printf("  try: curl -s '%s/admin/distill/node/%s' -H 'X-Tenant-ID: acme-corp'  # tenant root + L1 children", base, agent.TenantRootID())
	log.Printf("  try: curl -s '%s/admin/timeseries/keys' -H 'X-Tenant-ID: acme-corp'  # telemetry series keys", base)
	log.Printf("  try: curl -s -X POST %s/admin/timeseries/range -H 'Content-Type: application/json' -H 'X-Tenant-ID: acme-corp' -d '{\"key\":\"cpu.load\",\"bucketSeconds\":900,\"agg\":\"avg\"}'  # last 24h, 15m avg buckets", base)
}
