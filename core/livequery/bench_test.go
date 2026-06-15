package livequery_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/livequery/match"
	"github.com/xraph/fabriq/core/query"
)

// benchSort is the ordering shared by the live-query benchmarks.
var benchSort = []livequery.SortKey{{Column: "name"}}

// benchRows builds n ordered, active rows whose names sort by index, each with
// its keyset cursor precomputed — the shape a snapshot hands the window.
func benchRows(n int) []livequery.Row {
	rows := make([]livequery.Row, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("id-%06d", i)
		r := row(id, fmt.Sprintf("asset-%06d", i), float64(i))
		r.Cursor = livequery.SortKeyOf(r.Vals, benchSort, id)
		rows[i] = r
	}
	return rows
}

func benchQuery(limit int) livequery.LiveQuery {
	return livequery.LiveQuery{
		Entity: "asset",
		Sort:   benchSort,
		Limit:  limit,
		Where:  query.Where{query.Eq("status", "active")},
	}
}

// BenchmarkPredicateEval measures the per-event filter-match cost — the work
// the matcher does to decide whether a changed row belongs to a subscription.
// This is the unit the predicate index (P2) multiplies by the candidate count.
func BenchmarkPredicateEval(b *testing.B) {
	pred, err := match.Compile(query.Where{
		query.Eq("status", "active"),
		query.Gt("temp", 50.0),
		query.ILike("name", "asset-%"),
	})
	if err != nil {
		b.Fatal(err)
	}
	rowVals := map[string]any{"status": "active", "temp": 80.0, "name": "asset-000123"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !pred.Eval(rowVals) {
			b.Fatal("expected match")
		}
	}
}

// BenchmarkWindowApplyUpdate measures the per-event maintenance cost of an
// in-window UPDATE (same sort position, payload changed) as the window size
// grows. It captures the membership lookup + position search a single
// subscription pays for one change.
func BenchmarkWindowApplyUpdate(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			seed := benchRows(n + 16)
			pred, _ := match.Compile(query.Where{query.Eq("status", "active")})
			q := benchQuery(n)
			w, err := livequery.NewWindow(q, pred, seed, true, 16, &fakeRefiller{all: seed, sort: benchSort})
			if err != nil {
				b.Fatal(err)
			}
			target := seed[n/2]
			name, _ := target.Vals["name"].(string)
			ch := change(target.AggID, name, 1, "active", false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = w.Apply(context.Background(), ch)
			}
		})
	}
}

// BenchmarkWindowApplyChurn measures the full enter/evict ↔ leave/promote path:
// a row that toggles in and out of a 100-row window every event. complete=true
// keeps the work purely in memory (no refill), isolating the splice cost.
func BenchmarkWindowApplyChurn(b *testing.B) {
	seed := benchRows(116)
	pred, _ := match.Compile(query.Where{query.Eq("status", "active")})
	q := benchQuery(100)
	w, err := livequery.NewWindow(q, pred, seed, true, 16, &fakeRefiller{all: seed, sort: benchSort})
	if err != nil {
		b.Fatal(err)
	}
	const churnID, churnName = "id-churn", "asset-000050a" // sorts mid-window
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status := "active"
		if i%2 == 1 {
			status = "idle"
		}
		_ = w.Apply(context.Background(), change(churnID, churnName, 1, status, false))
	}
}

// BenchmarkWindowFanout measures the cost of routing ONE change to many
// subscriptions when every subscription is evaluated (the single-node P1
// model, with no predicate index). It quantifies why P2's content-based index
// matters: the per-event cost grows linearly in subscriber count here.
func BenchmarkWindowFanout(b *testing.B) {
	for _, m := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("subs=%d", m), func(b *testing.B) {
			seed := benchRows(20)
			pred, _ := match.Compile(query.Where{query.Eq("status", "active")})
			q := benchQuery(10)
			wins := make([]*livequery.Window, m)
			for j := range wins {
				w, err := livequery.NewWindow(q, pred, seed, true, 10, &fakeRefiller{all: seed, sort: benchSort})
				if err != nil {
					b.Fatal(err)
				}
				wins[j] = w
			}
			target := seed[5]
			name, _ := target.Vals["name"].(string)
			ch := change(target.AggID, name, 1, "active", false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, w := range wins {
					_ = w.Apply(context.Background(), ch)
				}
			}
		})
	}
}
