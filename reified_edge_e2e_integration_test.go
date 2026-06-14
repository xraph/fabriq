//go:build integration

package fabriq_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

// TestE2E_ReifiedEdgeUpsertAndDelete proves the reified-edge capability end to
// end: OpUpsert a Link (idempotent on its content-addressed id) projects to a
// single RELATED_TO relationship between two Assets; OpDelete (a payload-less
// event) removes exactly that relationship via RelDelete keyed on the AggID.
func TestE2E_ReifiedEdgeUpsertAndDelete(t *testing.T) {
	f, _, _ := graphE2E(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	a, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate, Payload: &domain.Asset{Name: "A"}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate, Payload: &domain.Asset{Name: "B"}})
	if err != nil {
		t.Fatal(err)
	}

	const linkID = "01HLINKE2E0000000000000001"
	// Upsert create (v1) then idempotent re-upsert (v2): same content-addressed id.
	for _, note := range []string{"x", "y"} {
		if _, err := f.Exec(ctx, command.Command{
			Entity: "link", Op: command.OpUpsert, AggID: linkID,
			Payload: &domain.Link{Kind: "RELATED_TO", SourceID: a.AggID, TargetID: b.AggID, Note: note},
		}); err != nil {
			t.Fatalf("upsert link (%s): %v", note, err)
		}
	}

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := f.WaitForProjection(waitCtx, "graph", "link", linkID, 2); err != nil {
		t.Fatalf("WaitForProjection (upsert): %v", err)
	}

	// Exactly one RELATED_TO edge A->B despite two upserts (MERGE keyed on r.id).
	var targets []string
	if err := f.Graph().Query(ctx,
		`MATCH (:Asset {id: $a})-[:RELATED_TO]->(b:Asset) RETURN b.id`,
		map[string]any{"a": a.AggID}, &targets); err != nil {
		t.Fatalf("graph query: %v", err)
	}
	if len(targets) != 1 || targets[0] != b.AggID {
		t.Fatalf("reified edge should exist exactly once -> [%s], got %v", b.AggID, targets)
	}

	// Delete the link: a payload-less event -> RelDelete by AggID.
	if _, err := f.Exec(ctx, command.Command{Entity: "link", Op: command.OpDelete, AggID: linkID}); err != nil {
		t.Fatalf("delete link: %v", err)
	}
	waitCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	defer cancel2()
	if err := f.WaitForProjection(waitCtx2, "graph", "link", linkID, 3); err != nil {
		t.Fatalf("WaitForProjection (delete): %v", err)
	}

	var afterDelete []string
	if err := f.Graph().Query(ctx,
		`MATCH (:Asset {id: $a})-[:RELATED_TO]->(b:Asset) RETURN b.id`,
		map[string]any{"a": a.AggID}, &afterDelete); err != nil {
		t.Fatalf("graph query after delete: %v", err)
	}
	if len(afterDelete) != 0 {
		t.Fatalf("edge should be gone after delete, got %v", afterDelete)
	}
}
