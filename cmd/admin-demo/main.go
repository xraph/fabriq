// Command admin-demo is a runnable demo server that mounts the fabriq adminapi
// extension against a live Postgres, so the fabriq-admin SPA can be exercised
// end-to-end against a real backend.
//
// It wires:
//
//   - a demo dynamic entity "product" (registry.DynamicSchema aggregate) plus
//     the adminapi plugin-remote schema (adminapi.PluginRemoteSpec);
//   - fabriq.Open (runs migrations) + EnsureDynamic for the dynamic tables;
//   - the fabriq forge extension (forgeext) and the adminapi extension;
//   - a tenant middleware that resolves the tenant from the X-Tenant-ID header
//     (the host's auth boundary — adminapi is auth-agnostic by design);
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

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/forgeext/adminapi"
)

const (
	defaultDSN  = "postgres://fabriq:fabriq@localhost:5433/fabriq?sslmode=disable"
	defaultAddr = ":8080"

	// productEntity is the demo dynamic entity type the SPA browses.
	productEntity = "product"
)

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

	// 1. Build the registry: the demo dynamic "product" entity + the adminapi
	//    plugin-remote schema. Both are DynamicSchema aggregates, written and
	//    read map-natively by the command plane and the relational querier.
	reg := registry.New()
	if err := reg.Register(productSpec()); err != nil {
		return err
	}
	if err := reg.Register(adminapi.PluginRemoteSpec()); err != nil {
		return err
	}
	if err := reg.Validate(); err != nil {
		return err
	}

	// 2. Open fabriq with a bare Postgres source of truth. Open dials the shard,
	//    assembles the facade AND runs fabriq's migrations (the command store,
	//    relational ports and projection bookkeeping tables). The Config shape
	//    mirrors cmd/api-example and cmd/fabriq: Postgres{DSN}.
	cfg := fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: dsn}}
	f, stores, err := fabriq.Open(ctx, reg, cfg)
	if err != nil {
		return err
	}

	// 3. EnsureDynamic creates the physical Postgres table for each dynamic
	//    entity (managed additive DDL). fabriq.Open exposes the primary shard's
	//    *postgres.Adapter via Stores.Postgres, which carries EnsureDynamic.
	for _, name := range []string{productEntity, adminapi.PluginRemoteEntityType} {
		ent, ok := reg.Get(name)
		if !ok {
			_ = f.Close()
			return errMissingEntity(name)
		}
		if err := stores.Postgres.EnsureDynamic(ctx, ent); err != nil {
			_ = f.Close()
			return err
		}
	}

	// 4. Idempotent seeding: ensure each demo tenant has a populated product
	//    catalogue (~60 rows) so the SPA's list endpoint pages. Uses the real
	//    fabric's command executor under a tenant-stamped context.
	for _, tid := range []string{"acme-corp", "globex"} {
		if err := seedProducts(ctx, f, tid, seedCount); err != nil {
			_ = f.Close()
			return err
		}
	}

	// The prep fabric has done its job (migrations, DDL, seeding). The serving
	// fabric is opened independently by the forgeext extension at app.Start;
	// close the prep handle so we don't hold a redundant pool.
	if err := f.Close(); err != nil {
		return err
	}

	// 5. Build the forge app and register the extensions. forgeext.Start opens
	//    its OWN fabriq facade (same DSN) and adminapi resolves that facade as
	//    its parent. No worker is enabled, so no Redis is required.
	app := forge.NewApp(forge.AppConfig{
		Name:        "fabriq-admin-demo",
		Version:     "0.1.0",
		HTTPAddress: addr,
	})

	// App-wide CORS so the SPA (any localhost dev port) can call the admin API
	// and its preflight OPTIONS gets a 204 short-circuit. UseGlobal runs before
	// routing, so the preflight never needs a registered OPTIONS route.
	app.Router().UseGlobal(corsMiddleware)

	fabricExt := forgeext.New(reg, forgeext.WithConfig(cfg))
	if err := app.RegisterExtension(fabricExt); err != nil {
		return err
	}

	// The adminapi extension is auth-agnostic: the host attaches tenant
	// resolution via WithRouteOptions. tenantMiddleware reads X-Tenant-ID and
	// stamps the tenant onto every admin route's request context.
	adminExt := adminapi.NewAdminAPI(fabricExt,
		adminapi.WithRouteOptions(forge.WithMiddleware(tenantMiddleware)),
	)
	if err := app.RegisterExtension(adminExt); err != nil {
		return err
	}

	logStartup(addr)
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
// X-Tenant-ID / Content-Type request headers, and short-circuits preflight
// OPTIONS with 204. A custom middleware (rather than middleware.CORS) is used so
// a bare OPTIONS preflight — without an Access-Control-Request-Method header —
// still returns 204 rather than falling through to routing.
func corsMiddleware(next forge.Handler) forge.Handler {
	return func(ctx forge.Context) error {
		w := ctx.Response()
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Content-Type,X-Tenant-ID")

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
	}
}

// errMissingEntity reports a registry lookup miss for a just-registered entity.
type errMissingEntity string

func (e errMissingEntity) Error() string {
	return "admin-demo: entity " + string(e) + " not found in registry after registration"
}

// logStartup prints the base URL and a couple of sample curl commands.
func logStartup(addr string) {
	base := "http://localhost" + addr
	log.Printf("admin-demo listening on %s (admin base: %s/admin)", addr, base)
	log.Printf("  seeded tenants: acme-corp, globex (%d products each)", seedCount)
	log.Printf("  try: curl -s %s/admin/meta -H 'X-Tenant-ID: acme-corp'", base)
	log.Printf("  try: curl -s '%s/admin/entities?type=product&limit=5' -H 'X-Tenant-ID: acme-corp'", base)
	log.Printf("  try: curl -s '%s/admin/entities?type=product&limit=50' -H 'X-Tenant-ID: acme-corp'  # paginate", base)
	log.Printf("  try: curl -s %s/admin/plugins -H 'X-Tenant-ID: acme-corp'", base)
}
