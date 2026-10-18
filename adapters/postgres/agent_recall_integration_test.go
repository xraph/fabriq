//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	coretest "github.com/xraph/fabriq/core/fabriqtest"
	"github.com/xraph/fabriq/core/query"
)

type recallPostgresFabric struct {
	query.Fabric
	h *dynWriteHarness
}

func (f recallPostgresFabric) Relational() query.RelationalQuerier { return f.h.A }
func (f recallPostgresFabric) Exec(ctx context.Context, c command.Command) (command.Result, error) {
	return f.h.X.Exec(ctx, c)
}

type recallEmbedder struct{}

func (recallEmbedder) Dims() int { return 3 }
func (recallEmbedder) Embed(_ context.Context, _ []string) ([][]float32, error) {
	return [][]float32{{1, 0, 0}}, nil
}

func TestAgentRecallFiltersPostgresRows(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")
	w := coretest.NewWorld(h.Reg)
	fab := recallPostgresFabric{Fabric: coretest.NewFabric(w), h: h}
	tk, err := agent.NewToolkit(fab, h.Reg, recallEmbedder{}, agent.Config{Strict: true, VectorDims: 3, Write: agent.WritePolicy{Allow: map[string][]command.Op{"orders": {command.OpCreate}}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"allowed", "revoked", "deleted", "foreign", "undiscovered"} {
		rowCtx := ctx
		if state == "foreign" {
			rowCtx = tctx(t, "other")
		}
		qty := int64(1)
		if state == "revoked" {
			qty = 0
		}
		if _, err = h.X.Exec(rowCtx, command.Command{Entity: "orders", Op: command.OpCreate, AggID: state, Payload: map[string]any{"sku": "lesson", "qty": qty}}); err != nil {
			t.Fatal(err)
		}
		if state == "deleted" {
			if _, err = h.X.Exec(ctx, command.Command{Entity: "orders", Op: command.OpDelete, AggID: state}); err != nil {
				t.Fatal(err)
			}
		}
		if state != "undiscovered" {
			if err = w.Vector.Upsert(ctx, "orders", state, []float32{1, 0, 0}, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	pack, err := tk.Recall(ctx, agent.RecallRequest{Query: "lesson", Budget: 10000, K: 10, Entities: []string{"orders"}, Filters: query.Where{query.Eq("qty", int64(1))}})
	if err != nil {
		t.Fatal(err)
	}
	if len(pack.Items) != 1 || pack.Items[0].ID != "allowed" {
		t.Fatalf("unexpected RLS-filtered pack %+v", pack)
	}
	zero := int64(0)
	req := agent.RememberRequest{Entity: "orders", Op: "create", AggID: "publication", ExpectedVersion: &zero, Payload: []byte(`{"sku":"original","qty":1}`)}
	if _, err = tk.Remember(ctx, req); err != nil {
		t.Fatal(err)
	}
	req.Payload = []byte(`{"sku":"replacement","qty":0}`)
	if _, err = tk.Remember(ctx, req); !errors.Is(err, fabriqerr.ErrVersionConflict) {
		t.Fatalf("duplicate create: %v", err)
	}
	var row map[string]any
	if err = h.A.Get(ctx, "orders", "publication", &row); err != nil {
		t.Fatal(err)
	}
	if row["sku"] != "original" {
		t.Fatalf("duplicate changed authoritative row: %v", row)
	}
}
