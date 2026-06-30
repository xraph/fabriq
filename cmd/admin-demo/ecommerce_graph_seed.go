package main

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// E-commerce graph labels and relationship types. They extend the product/
// category graph seedGraph already builds (IN_CATEGORY / RELATED_TO) with the
// customer/order cross-type edges, forming a connected mini e-commerce graph so
// traverse/neighbors from any node return something interesting.
const (
	graphCustomerLabel = "Customer"
	graphOrderLabel    = "Order"
	graphCountryLabel  = "Country"

	relPlaced   = "PLACED"   // (customer)-[:PLACED]->(order)
	relContains = "CONTAINS" // (order)-[:CONTAINS]->(product)
	relLivesIn  = "LIVES_IN" // (customer)-[:LIVES_IN]->(country)
)

// seedEcommerceGraph idempotently adds the customer/order cross-type layer to
// tenant tid's live graph (registry.GraphName(tid) == "tenant_<id>") via the
// DIRECT projection write path Graph().ApplyMutations(ctx, "", muts) — the same
// graph the read endpoints query and the same shape seedGraph uses for products.
//
// It writes, on top of the existing product/category graph:
//
//   - a Customer node per customer (props name/tier/country);
//   - an Order node per order (props status/total);
//   - a Country node per distinct customer country;
//   - (customer)-[:PLACED]->(order)   — one per order, to its customer;
//   - (order)-[:CONTAINS]->(product)  — one per order, to its product;
//   - (customer)-[:LIVES_IN]->(country) — one per customer.
//
// Every mutation renders to a version-gated MERGE in the falkordb dialect, so
// re-running on every startup is safe. It returns the nodes and edges written.
//
// Note: the order->product CONTAINS edge points at the SAME Product node id the
// product graph already created (the order's productId is a real product id), so
// the two layers are genuinely connected — a traverse from a customer reaches
// products (and from there their categories) within a couple of hops.
func seedEcommerceGraph(ctx context.Context, f *fabriq.Fabriq, tid string) (nodes, edges int, err error) {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return 0, 0, fmt.Errorf("admin-demo: ecommerce graph tenant %q: %w", tid, err)
	}

	var customers []map[string]any
	if lerr := f.Relational().List(tctx, customerEntity, query.ListQuery{Limit: 1000, OrderBy: "id ASC"}, &customers); lerr != nil {
		return 0, 0, fmt.Errorf("admin-demo: list customers for graph %q: %w", tid, lerr)
	}
	var orders []map[string]any
	if lerr := f.Relational().List(tctx, orderEntity, query.ListQuery{Limit: 1000, OrderBy: "id ASC"}, &orders); lerr != nil {
		return 0, 0, fmt.Errorf("admin-demo: list orders for graph %q: %w", tid, lerr)
	}
	if len(customers) == 0 || len(orders) == 0 {
		return 0, 0, nil
	}

	muts := make([]projection.Mutation, 0, len(customers)*2+len(orders)*3+8)

	// 1. Customer nodes + their country node + LIVES_IN edge. Country nodes are
	//    deduped by name (MERGE is idempotent, so repeating the same id is fine).
	seenCountry := map[string]bool{}
	for _, row := range customers {
		id, _ := row["id"].(string)
		if id == "" {
			continue
		}
		version := toInt64(row["version"])
		country := asString(row["country"])

		muts = append(muts, projection.NodeUpsert{
			Label: graphCustomerLabel,
			ID:    id,
			Props: map[string]any{
				"name":    asString(row["name"]),
				"tier":    asString(row["tier"]),
				"country": country,
			},
			Version: version,
		})
		nodes++

		if country != "" {
			if !seenCountry[country] {
				muts = append(muts, projection.NodeUpsert{
					Label:   graphCountryLabel,
					ID:      countryID(country),
					Props:   map[string]any{"name": country},
					Version: 1,
				})
				nodes++
				seenCountry[country] = true
			}
			muts = append(muts, projection.EdgeUpsert{
				Rel:       relLivesIn,
				FromLabel: graphCustomerLabel,
				FromID:    id,
				ToLabel:   graphCountryLabel,
				ToID:      countryID(country),
				Version:   version,
			})
			edges++
		}
	}

	// 2. Order nodes + PLACED (customer->order) + CONTAINS (order->product).
	//    PLACED is reified (RelUpsert) so a customer can own many orders without
	//    the FK pruning EdgeUpsert applies. CONTAINS is reified too (an order may
	//    reference any product, and a product may be contained by many orders).
	for _, row := range orders {
		id, _ := row["id"].(string)
		if id == "" {
			continue
		}
		version := toInt64(row["version"])
		custID := asString(row["customer_id"])
		prodID := asString(row["product_id"])

		muts = append(muts, projection.NodeUpsert{
			Label: graphOrderLabel,
			ID:    id,
			Props: map[string]any{
				"status": asString(row["status"]),
				"total":  row["total"],
			},
			Version: version,
		})
		nodes++

		if custID != "" {
			muts = append(muts, projection.RelUpsert{
				ID:        "placed-" + custID + "-" + id,
				Type:      relPlaced,
				FromLabel: graphCustomerLabel,
				FromID:    custID,
				ToLabel:   graphOrderLabel,
				ToID:      id,
				Version:   version,
			})
			edges++
		}
		if prodID != "" {
			muts = append(muts, projection.RelUpsert{
				ID:        "contains-" + id + "-" + prodID,
				Type:      relContains,
				FromLabel: graphOrderLabel,
				FromID:    id,
				ToLabel:   graphProductLabel,
				ToID:      prodID,
				Version:   version,
			})
			edges++
		}
	}

	if applyErr := f.Graph().ApplyMutations(tctx, "", muts); applyErr != nil {
		return 0, 0, fmt.Errorf("admin-demo: ecommerce graph apply for %q: %w", tid, applyErr)
	}
	return nodes, edges, nil
}

// countryID derives a stable, identifier-safe node id for a country name.
func countryID(name string) string { return "country-" + name }
