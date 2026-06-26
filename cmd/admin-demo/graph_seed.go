package main

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// graphCategories are the demo category nodes every product is bucketed into.
// Each product is assigned a category by a stable hash of its id, so the
// assignment is deterministic and idempotent across restarts.
var graphCategories = []string{"Widgets", "Gadgets", "Tools"}

// Graph labels and relationship types used by the demo knowledge graph. They
// satisfy the falkordb adapter's identifier validation (letters/underscore).
const (
	graphProductLabel  = "Product"
	graphCategoryLabel = "Category"
	relInCategory      = "IN_CATEGORY"
	relRelatedTo       = "RELATED_TO"
)

// seedGraph idempotently builds the demo knowledge graph for tenant tid in
// FalkorDB via the DIRECT projection write path: Graph().ApplyMutations(ctx,
// "", muts). Passing target "" resolves to the tenant's live graph
// (registry.GraphName(tid) == "tenant_<id>") — the SAME graph the read
// endpoints query — so seeded relationships are visible to neighbors/traverse.
//
// The graph it builds:
//
//   - one Category node per graphCategories entry;
//   - each existing product as a Product node (props name/sku/status);
//   - one IN_CATEGORY edge product -> category (category chosen by hash of the
//     product id), modelled as an EdgeUpsert (FK semantics: one category per
//     product);
//   - a RELATED_TO relationship from each product to the next product sharing
//     its category, modelled as a reified RelUpsert (keyed by its own id so a
//     product may carry several without the FK pruning EdgeUpsert applies).
//
// Every mutation renders to a version-gated MERGE in the falkordb dialect, so
// re-running on every startup is safe. It returns the number of nodes and edges
// it wrote.
func seedGraph(ctx context.Context, f *fabriq.Fabriq, tid string) (nodes, edges int, err error) {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return 0, 0, fmt.Errorf("admin-demo: graph tenant %q: %w", tid, err)
	}

	var rows []map[string]any
	if lerr := f.Relational().List(tctx, productEntity, query.ListQuery{Limit: 1000, OrderBy: "id ASC"}, &rows); lerr != nil {
		return 0, 0, fmt.Errorf("admin-demo: list products for graph %q: %w", tid, lerr)
	}
	if len(rows) == 0 {
		return 0, 0, nil
	}

	muts := make([]projection.Mutation, 0, len(graphCategories)+len(rows)*3)

	// 1. Category nodes.
	for _, cat := range graphCategories {
		muts = append(muts, projection.NodeUpsert{
			Label:   graphCategoryLabel,
			ID:      categoryID(cat),
			Props:   map[string]any{"name": cat},
			Version: 1,
		})
		nodes++
	}

	// 2. Product nodes + their category assignment. Track category -> ordered
	//    product ids so we can wire RELATED_TO within a category afterwards.
	byCategory := map[string][]string{}
	for _, row := range rows {
		id, _ := row["id"].(string)
		if id == "" {
			continue
		}
		version := toInt64(row["version"])
		catName := graphCategories[hashIndex(id, len(graphCategories))]

		muts = append(muts, projection.NodeUpsert{
			Label: graphProductLabel,
			ID:    id,
			Props: map[string]any{
				"name":   asString(row["name"]),
				"sku":    asString(row["sku"]),
				"status": asString(row["status"]),
			},
			Version: version,
		})
		nodes++

		muts = append(muts, projection.EdgeUpsert{
			Rel:       relInCategory,
			FromLabel: graphProductLabel,
			FromID:    id,
			ToLabel:   graphCategoryLabel,
			ToID:      categoryID(catName),
			Version:   version,
		})
		edges++

		byCategory[catName] = append(byCategory[catName], id)
	}

	// 3. RELATED_TO edges chaining products within each category (each product to
	//    the next in its category, and the last back to the first to close the
	//    loop when a category has >= 2 members) — enough that neighbors/traverse
	//    return non-trivial subgraphs. Reified (RelUpsert) so multiple relations
	//    per node are retained.
	for _, ids := range byCategory {
		if len(ids) < 2 {
			continue
		}
		for i, from := range ids {
			to := ids[(i+1)%len(ids)]
			if from == to {
				continue
			}
			muts = append(muts, projection.RelUpsert{
				ID:        relatedID(from, to),
				Type:      relRelatedTo,
				FromLabel: graphProductLabel,
				FromID:    from,
				ToLabel:   graphProductLabel,
				ToID:      to,
				Version:   1,
			})
			edges++
		}
	}

	if applyErr := f.Graph().ApplyMutations(tctx, "", muts); applyErr != nil {
		return 0, 0, fmt.Errorf("admin-demo: graph apply for %q: %w", tid, applyErr)
	}
	return nodes, edges, nil
}

// categoryID derives a stable, identifier-safe node id for a category name.
func categoryID(name string) string { return "cat-" + name }

// relatedID derives a stable id for a RELATED_TO relationship between two
// product ids (used as r.id for the reified RelUpsert).
func relatedID(from, to string) string { return "rel-" + from + "-" + to }

// hashIndex returns a stable index in [0, n) derived from s via FNV-1a, used to
// bucket a product into a category deterministically.
func hashIndex(s string, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % uint32(n))
}
