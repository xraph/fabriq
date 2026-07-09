package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// New demo dynamic entities and their search index base names. They mirror the
// "product" pattern exactly: DynamicSchema aggregates, created physically via
// EnsureDynamic in main.go, written through the command executor under a
// tenant-stamped context, then indexed into Search (ES) and Vector (pgvector)
// via the same direct write paths products use.
const (
	customerEntity = "customer"
	orderEntity    = "order"

	// customerSearchIndex / orderSearchIndex are the logical search index base
	// names. The elastic adapter derives the per-tenant alias
	// (fabriq_<tenant>_<base>) from these, exactly as it does for "products".
	customerSearchIndex = "customers"
	orderSearchIndex    = "orders"

	// customerCount / orderCount are the per-tenant seed sizes.
	customerCount = 15
	orderCount    = 40
)

// customerTiers / orderStatuses cycle a few values across seeded rows so the SPA
// has something to group/filter on.
var (
	customerTiers = []string{"free", "pro", "enterprise"}
	orderStatuses = []string{"pending", "paid", "shipped", "cancelled"}
)

// customerSpec returns the demo dynamic entity spec for "customer". Like
// productSpec it is a DynamicSchema aggregate, search-indexed on its text fields
// so /search?type=customer works and the per-type capability probe reports
// search:true.
func customerSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: customerEntity,
		Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_customers",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "email", Type: registry.ColText, NotNull: true},
				{Name: "tier", Type: registry.ColText},
				{Name: "country", Type: registry.ColText},
			},
		},
		// Analytics: name/tier/country cross the trust boundary; EMAIL is
		// pseudonymized (salted hash) so operators can count-distinct / join on it
		// across tenants without co-locating the raw address. Everything else is
		// stripped. This is the Privacy showcase in the Analytics console.
		Analytics: &registry.AnalyticsSpec{
			Include: []string{"name", "tier", "country"},
			Hash:    []string{"email"},
		},
		Search: registry.SearchSpec{
			Index:  customerSearchIndex,
			Fields: []string{"name", "email", "tier", "country"},
		},
		// ByTenant is required for the live-query feed (changes:{tenant}:tenant:{tenant}).
		Subscribe: []registry.Scope{registry.ByTenant, registry.ByID},
		// Live opts the customer entity into the live query engine (adminapi
		// POST /admin/live). Requires Redis (the change feed).
		Live: &registry.LiveSpec{
			Filterable: []string{"name", "email", "tier", "country"},
			Sortable:   []string{"name", "email", "tier"},
			MaxWindow:  500,
		},
		// Distill opts customers into context distillation alongside products:
		// name+email+tier+country forms the L0 source text, and "tier" is the scope
		// that groups customers into L1 scope-backbone digest nodes (one per tier).
		// The tenant root (L2) then rolls up BOTH the product-status scopes and the
		// customer-tier scopes, so the digest tree spans two source entity types.
		Distill: &registry.DistillSpec{
			SourceFields: []string{"name", "email", "tier", "country"},
			Scopes:       []string{"tier"},
		},
	}
}

// orderSpec returns the demo dynamic entity spec for "order". Each seeded order
// references a real customer id and product id (string columns), so the graph
// step can wire (customer)-[:PLACED]->(order)-[:CONTAINS]->(product).
func orderSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: orderEntity,
		Kind: registry.KindAggregate,
		// DB columns are snake_case (fabriq house rule: DB columns stay snake_case
		// even though the SPA sees camelCase via the adminapi JSON layer). Postgres
		// folds unquoted identifiers to lowercase, so camelCase column names would
		// not round-trip; the relational scan keys the row map by the literal
		// column name, so the seed reads them as snake_case below.
		Schema: &registry.DynamicSchema{
			Table: "ds_orders",
			Columns: []registry.DynamicColumn{
				{Name: "customer_id", Type: registry.ColText, NotNull: true},
				{Name: "product_id", Type: registry.ColText, NotNull: true},
				{Name: "total", Type: registry.ColFloat},
				{Name: "status", Type: registry.ColText},
				{Name: "created_at", Type: registry.ColText},
			},
		},
		// Analytics: status/total/product_id cross the trust boundary; CUSTOMER_ID
		// is pseudonymized so orders can be grouped per customer across the fleet
		// without co-locating the raw id (the same salted hash the customer spec
		// applies to email, so cross-entity joins on the pseudonym still work).
		Analytics: &registry.AnalyticsSpec{
			Include: []string{"status", "total", "product_id"},
			Hash:    []string{"customer_id"},
		},
		Search: registry.SearchSpec{
			Index:  orderSearchIndex,
			Fields: []string{"status", "customer_id", "product_id"},
		},
		// ByTenant is required for the live-query feed (changes:{tenant}:tenant:{tenant}).
		Subscribe: []registry.Scope{registry.ByTenant, registry.ByID},
		// Live opts the order entity into the live query engine (adminapi
		// POST /admin/live). Requires Redis (the change feed).
		Live: &registry.LiveSpec{
			Filterable: []string{"status", "customer_id", "product_id", "total"},
			Sortable:   []string{"status", "total"},
			MaxWindow:  500,
		},
	}
}

// customerNames / customerCountries are small varied pools the seed draws from
// (deterministically, indexed by row number) so each tenant gets realistic,
// distinct-looking values. Different tenants offset into the pools so the data
// visibly differs per tenant.
var (
	customerFirst = []string{
		"Ada", "Boris", "Carmen", "Dmitri", "Elena", "Farid", "Grace", "Hassan",
		"Ingrid", "Javier", "Keiko", "Lars", "Mira", "Nadia", "Omar", "Priya",
		"Quentin", "Rosa", "Sven", "Tara",
	}
	customerLast = []string{
		"Okafor", "Petrov", "Reyes", "Volkov", "Nakamura", "Haddad", "Lindqvist",
		"Mensah", "Costa", "Andersson", "Singh", "Romano", "Diallo", "Novak",
	}
	customerCountries = []string{
		"USA", "Germany", "Japan", "Brazil", "India", "Sweden", "Nigeria",
		"France", "Canada", "Spain",
	}
)

// seedCustomers idempotently ensures tenant tid has at least customerCount
// customer rows. It mirrors seedProducts: count existing rows, skip when already
// populated, otherwise top up via the command executor under a tenant-stamped
// context. Values are deterministic per (tenant, row) so restarts converge.
func seedCustomers(ctx context.Context, f *fabriq.Fabriq, tid string, want int) error {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return fmt.Errorf("admin-demo: customer seed tenant %q: %w", tid, err)
	}

	existing, err := countEntity(tctx, f, customerEntity)
	if err != nil {
		return fmt.Errorf("admin-demo: count customers for %q: %w", tid, err)
	}
	if existing >= want {
		return nil // already seeded — safe to re-run on every startup
	}

	off := tenantOffset(tid)
	for i := existing; i < want; i++ {
		n := i + 1
		first := customerFirst[(i+off)%len(customerFirst)]
		last := customerLast[(i+off)%len(customerLast)]
		name := fmt.Sprintf("%s %s", first, last)
		email := fmt.Sprintf("%s.%s@%s.example", strings.ToLower(first), strings.ToLower(last), tid)
		tier := customerTiers[(i+off)%len(customerTiers)]
		country := customerCountries[(i+off)%len(customerCountries)]

		_, execErr := f.Exec(tctx, command.Command{
			Entity: customerEntity,
			Op:     command.OpCreate,
			Payload: map[string]any{
				"name":    name,
				"email":   email,
				"tier":    tier,
				"country": country,
			},
		})
		if execErr != nil {
			return fmt.Errorf("admin-demo: seed customer %d for %q: %w", n, tid, execErr)
		}
	}
	return nil
}

// seedOrders idempotently ensures tenant tid has at least orderCount order rows.
// Each order references a real seeded customer id and product id (round-robined
// over the tenant's existing customers/products), so the order->customer and
// order->product references are valid and the graph wiring forms a real
// e-commerce subgraph. Skips when already populated.
func seedOrders(ctx context.Context, f *fabriq.Fabriq, tid string, want int) error {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return fmt.Errorf("admin-demo: order seed tenant %q: %w", tid, err)
	}

	existing, err := countEntity(tctx, f, orderEntity)
	if err != nil {
		return fmt.Errorf("admin-demo: count orders for %q: %w", tid, err)
	}
	if existing >= want {
		return nil // already seeded
	}

	custIDs, err := entityIDs(tctx, f, customerEntity)
	if err != nil {
		return fmt.Errorf("admin-demo: list customers for orders %q: %w", tid, err)
	}
	prodIDs, err := entityIDs(tctx, f, productEntity)
	if err != nil {
		return fmt.Errorf("admin-demo: list products for orders %q: %w", tid, err)
	}
	if len(custIDs) == 0 || len(prodIDs) == 0 {
		return nil // nothing to reference yet
	}

	off := tenantOffset(tid)
	for i := existing; i < want; i++ {
		n := i + 1
		custID := custIDs[(i+off)%len(custIDs)]
		prodID := prodIDs[(i*7+off)%len(prodIDs)]
		status := orderStatuses[(i+off)%len(orderStatuses)]
		total := float64((i+1)*13%500) + 19.99
		// Deterministic ISO-ish createdAt spread across a couple of months.
		day := (i % 28) + 1
		month := (i/28)%12 + 1
		createdAt := fmt.Sprintf("2026-%02d-%02dT10:%02d:00Z", month, day, n%60)

		_, execErr := f.Exec(tctx, command.Command{
			Entity: orderEntity,
			Op:     command.OpCreate,
			Payload: map[string]any{
				"customer_id": custID,
				"product_id":  prodID,
				"total":       total,
				"status":      status,
				"created_at":  createdAt,
			},
		})
		if execErr != nil {
			return fmt.Errorf("admin-demo: seed order %d for %q: %w", n, tid, execErr)
		}
	}
	return nil
}

// indexCustomers populates Search (ES) and Vector (pgvector) for tenant tid's
// customers via the same direct write paths indexProducts uses. The search doc
// carries the declared fields plus id/tenant/version; the embedding text is a
// name+email+tier+country blob. Idempotent (external_gte ES bulk + ON CONFLICT
// vector upsert). Returns the number of rows indexed.
func indexCustomers(ctx context.Context, f *fabriq.Fabriq, emb agent.Embedder, tid string) (int, error) {
	return indexDynamic(ctx, f, emb, tid, customerEntity, customerSearchIndex,
		[]string{"name", "email", "tier", "country"},
		func(row map[string]any) string {
			return strings.TrimSpace(strings.Join([]string{
				asString(row["name"]), asString(row["email"]),
				asString(row["tier"]), asString(row["country"]),
			}, " "))
		},
		func(row map[string]any) map[string]any {
			return map[string]any{
				"tier":    asString(row["tier"]),
				"country": asString(row["country"]),
			}
		},
	)
}

// indexOrders populates Search (ES) and Vector (pgvector) for tenant tid's
// orders. The embedding text is a status+id blob (orders have little free text),
// which is enough for the demo's lexical embedder. Returns rows indexed.
func indexOrders(ctx context.Context, f *fabriq.Fabriq, emb agent.Embedder, tid string) (int, error) {
	return indexDynamic(ctx, f, emb, tid, orderEntity, orderSearchIndex,
		[]string{"status", "customer_id", "product_id"},
		func(row map[string]any) string {
			return strings.TrimSpace(strings.Join([]string{
				asString(row["status"]), asString(row["id"]),
				asString(row["customer_id"]), asString(row["product_id"]),
			}, " "))
		},
		func(row map[string]any) map[string]any {
			return map[string]any{
				"status":      asString(row["status"]),
				"customer_id": asString(row["customer_id"]),
			}
		},
	)
}

// indexDynamic is the generic search+vector indexer products/customers/orders
// all share: it lists the entity's rows for the tenant, builds one DocIndex per
// row (the named docFields plus id/tenant/version), bulk-indexes them into ES,
// then embeds a per-row text blob (textOf) and upserts each vector under the
// entity type keyed by row id with metaOf metadata. Mirrors indexProducts.
func indexDynamic(
	ctx context.Context,
	f *fabriq.Fabriq,
	emb agent.Embedder,
	tid, entity, indexBase string,
	docFields []string,
	textOf func(map[string]any) string,
	metaOf func(map[string]any) map[string]any,
) (int, error) {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return 0, fmt.Errorf("admin-demo: index %s tenant %q: %w", entity, tid, err)
	}

	var rows []map[string]any
	if lerr := f.Relational().List(tctx, entity, query.ListQuery{Limit: 1000}, &rows); lerr != nil {
		return 0, fmt.Errorf("admin-demo: list %s for %q: %w", entity, tid, lerr)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	muts := make([]projection.Mutation, 0, len(rows))
	texts := make([]string, 0, len(rows))
	type rowRef struct {
		id   string
		meta map[string]any
	}
	refs := make([]rowRef, 0, len(rows))

	for _, row := range rows {
		id, _ := row["id"].(string)
		if id == "" {
			continue
		}
		version := toInt64(row["version"])

		doc := map[string]any{
			registry.ColumnID:      id,
			registry.ColumnTenant:  tid,
			registry.ColumnVersion: version,
		}
		for _, fld := range docFields {
			doc[fld] = row[fld]
		}
		muts = append(muts, projection.DocIndex{
			Index:   indexBase,
			ID:      id,
			Doc:     doc,
			Version: version,
		})

		texts = append(texts, textOf(row))
		refs = append(refs, rowRef{id: id, meta: metaOf(row)})
	}

	if serr := f.Search().ApplyMutations(tctx, "", muts); serr != nil {
		return 0, fmt.Errorf("admin-demo: search index %s for %q: %w", entity, tid, serr)
	}

	vectors, eerr := emb.Embed(tctx, texts)
	if eerr != nil {
		return 0, fmt.Errorf("admin-demo: embed %s for %q: %w", entity, tid, eerr)
	}
	if len(vectors) != len(refs) {
		return 0, fmt.Errorf("admin-demo: embedder returned %d vectors for %d %s rows (%q)", len(vectors), len(refs), entity, tid)
	}
	for i, ref := range refs {
		if uerr := f.Vector().Upsert(tctx, entity, ref.id, vectors[i], ref.meta); uerr != nil {
			return 0, fmt.Errorf("admin-demo: vector upsert %s %s for %q: %w", entity, ref.id, tid, uerr)
		}
	}

	return len(refs), nil
}

// countEntity returns the number of rows of the given entity type visible to the
// tenant in ctx, using the relational list path (matches countProducts).
func countEntity(ctx context.Context, f *fabriq.Fabriq, entity string) (int, error) {
	var rows []map[string]any
	if err := f.Relational().List(ctx, entity, query.ListQuery{Limit: 1000}, &rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

// entityIDs returns the ids of all rows of the given entity for the tenant in
// ctx, ordered by id so the assignment of orders to customers/products is stable
// across restarts.
func entityIDs(ctx context.Context, f *fabriq.Fabriq, entity string) ([]string, error) {
	var rows []map[string]any
	if err := f.Relational().List(ctx, entity, query.ListQuery{Limit: 1000, OrderBy: "id ASC"}, &rows); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id, _ := row["id"].(string); id != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// tenantOffset gives each tenant a stable, well-spread integer offset so the
// deterministic value pools (names/tiers/countries/statuses) start at a
// different point per tenant — making tenant isolation visible in the dashboard.
// It hashes over a large modulus (rather than mod len(pool)) so two tenants are
// very unlikely to land on the same offset for every pool, and the per-pool
// modulo in the callers spreads the larger value across each pool's length.
func tenantOffset(tid string) int {
	return hashIndex(tid, 9973) // large prime — distinct per tenant across pools
}
