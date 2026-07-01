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

// seedCount is how many product rows each demo tenant gets. It is comfortably
// above the adminapi default list page size (50) so the SPA exercises cursor
// pagination on the very first browse.
const seedCount = 60

// productStatuses cycles a few statuses across the seeded rows so the SPA has
// something to group/filter on.
var productStatuses = []string{"active", "draft", "archived"}

// seedProducts idempotently ensures tenant tid has at least want product rows.
// It runs every startup: it first counts existing rows for the tenant and skips
// seeding entirely when the catalogue is already populated, otherwise it tops up
// to want rows via the command executor under a tenant-stamped context.
//
// Writes go through fabriq.Exec (command.OpCreate), the only sanctioned write
// path for aggregates; reads go through the relational querier. Both are
// tenant-scoped structurally — the tenant comes from the context, not the row.
func seedProducts(ctx context.Context, f *fabriq.Fabriq, tid string, want int) error {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return fmt.Errorf("admin-demo: seed tenant %q: %w", tid, err)
	}

	existing, err := countProducts(tctx, f)
	if err != nil {
		return fmt.Errorf("admin-demo: count products for %q: %w", tid, err)
	}
	if existing >= want {
		return nil // already seeded — safe to re-run on every startup
	}

	for i := existing; i < want; i++ {
		n := i + 1
		_, execErr := f.Exec(tctx, command.Command{
			Entity: productEntity,
			Op:     command.OpCreate,
			Payload: map[string]any{
				"name":   fmt.Sprintf("%s Product %03d", tid, n),
				"sku":    fmt.Sprintf("%s-SKU-%04d", tid, n),
				"price":  float64(n)*1.5 + 9.99,
				"status": productStatuses[i%len(productStatuses)],
			},
		})
		if execErr != nil {
			return fmt.Errorf("admin-demo: seed product %d for %q: %w", n, tid, execErr)
		}
	}
	return nil
}

// countProducts returns the number of product rows visible to the tenant in ctx.
// It pages through the rows with a generous cap; the demo seed sizes (~60) are
// well within a single page.
func countProducts(ctx context.Context, f *fabriq.Fabriq) (int, error) {
	var rows []map[string]any
	q := query.ListQuery{Limit: 1000}
	if err := f.Relational().List(ctx, productEntity, q, &rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

// indexProducts populates the Search (Elasticsearch) and Vector (pgvector)
// planes for tenant tid's products using DIRECT write paths — the same paths the
// async projection worker would drive, but invoked inline so the demo needs no
// Redis/worker:
//
//   - Search: f.Search().ApplyMutations(ctx, "", []DocIndex{...}). target "" routes
//     to the tenant's live versioned index and keeps the read alias pointed at it.
//     The DocIndex Doc mirrors core/projection.SearchApplier (the declared search
//     fields plus id/tenant_id/version) so a direct write is identical to what the
//     worker would produce.
//   - Vector: f.Vector().Upsert(ctx, "product", id, embedding, meta). The embedding
//     comes from the local deterministic embedder over a name+sku+status text blob.
//
// Both are idempotent: ES bulk ops carry version_type=external_gte (stale replays
// are version conflicts treated as success) and the vector upsert is ON CONFLICT
// DO UPDATE, so re-running on every startup is safe. It returns the number of
// products indexed for the tenant.
func indexProducts(ctx context.Context, f *fabriq.Fabriq, emb agent.Embedder, tid string) (int, error) {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return 0, fmt.Errorf("admin-demo: index tenant %q: %w", tid, err)
	}

	var rows []map[string]any
	if lerr := f.Relational().List(tctx, productEntity, query.ListQuery{Limit: 1000}, &rows); lerr != nil {
		return 0, fmt.Errorf("admin-demo: list products for %q: %w", tid, lerr)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	// Build the search mutations and the per-row embedding texts in one pass.
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
			"name":                 row["name"],
			"sku":                  row["sku"],
			"status":               row["status"],
			registry.ColumnID:      id,
			registry.ColumnTenant:  tid,
			registry.ColumnVersion: version,
		}
		muts = append(muts, projection.DocIndex{
			Index:   productSearchIndex,
			ID:      id,
			Doc:     doc,
			Version: version,
		})

		texts = append(texts, productText(row))
		refs = append(refs, rowRef{
			id: id,
			meta: map[string]any{
				"sku":    asString(row["sku"]),
				"status": asString(row["status"]),
			},
		})
	}

	// Index into Search (Elasticsearch) in one bulk request.
	if serr := f.Search().ApplyMutations(tctx, "", muts); serr != nil {
		return 0, fmt.Errorf("admin-demo: search index for %q: %w", tid, serr)
	}

	// Embed all texts, then upsert each embedding into pgvector under entity
	// "product" keyed by the row id (so similar-to-entity by id resolves it).
	vectors, eerr := emb.Embed(tctx, texts)
	if eerr != nil {
		return 0, fmt.Errorf("admin-demo: embed products for %q: %w", tid, eerr)
	}
	if len(vectors) != len(refs) {
		return 0, fmt.Errorf("admin-demo: embedder returned %d vectors for %d rows (%q)", len(vectors), len(refs), tid)
	}
	for i, ref := range refs {
		if uerr := f.Vector().Upsert(tctx, productEntity, ref.id, vectors[i], ref.meta); uerr != nil {
			return 0, fmt.Errorf("admin-demo: vector upsert %s for %q: %w", ref.id, tid, uerr)
		}
	}

	return len(refs), nil
}

// productText builds the text blob a product row is embedded from: its name, sku
// and status joined. Lexical overlap in these fields is what brings a free-text
// vector query (e.g. "widget") close to matching rows under the demo embedder.
func productText(row map[string]any) string {
	parts := []string{asString(row["name"]), asString(row["sku"]), asString(row["status"])}
	return strings.TrimSpace(strings.Join(parts, " "))
}

// asString coerces a map value to its string form ("" when nil or non-string).
func asString(v any) string {
	s, _ := v.(string)
	return s
}

// toInt64 coerces a relational row's version value (which may arrive as int64,
// int, or float64 depending on the scan path) to int64, defaulting to 1 so a
// direct DocIndex always carries a positive external version for gating.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		if n > 0 {
			return n
		}
	case int:
		if n > 0 {
			return int64(n)
		}
	case float64:
		if n > 0 {
			return int64(n)
		}
	}
	return 1
}
