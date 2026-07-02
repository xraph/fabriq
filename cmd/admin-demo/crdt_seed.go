package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/crdt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// pageEntity is the demo KindDocument (CRDT) entity the document-plane endpoints
// read. Registering it (a) flips the registry-derived crdt capability flag to
// true and (b) lets the postgres document store accept "page/<id>" doc ids
// (DocStore.splitDocID requires a registered KindDocument entity).
const pageEntity = "page"

// demoPageID is the stable, idempotent demo document id seeded on every boot.
// It is the "<id>" half of the "page/<id>" document id.
const demoPageID = "welcome"

// pageSpec returns the demo KindDocument entity spec for "page". A CRDTSpec is
// mandatory for KindDocument (registry validation rejects KindDocument without
// one); the engine reference is grove-crdt — the merge engine the postgres
// DocStore folds updates through. The relational columns are the targets the
// quiet-window materializer stamps from the merged CRDT field map; the read
// endpoints (Snapshot / update log) do not require them, but they make a
// materialized "page" row render in the entity browser too.
func pageSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: pageEntity,
		Kind: registry.KindDocument,
		CRDT: &registry.CRDTSpec{
			Engine:        "grove-crdt",
			SnapshotEvery: 64,
			QuietWindow:   2_000_000_000, // 2s, matching the example "page" entity
		},
		Schema: &registry.DynamicSchema{
			Table: "ds_pages",
			Columns: []registry.DynamicColumn{
				{Name: "title", Type: registry.ColText},
				{Name: "body", Type: registry.ColText},
			},
		},
	}
}

// crdtLWWUpdate encodes one LWW field change as the postgres DocStore's update
// wire format: a JSON-encoded non-empty []crdt.ChangeRecord. The HLC wall clock
// orders concurrent writes; a higher hlcWall wins. This mirrors the integration
// test helper of the same name (adapters/postgres/document_scope_integration_test.go).
func crdtLWWUpdate(table, docID, field string, value any, hlcWall int64) ([]byte, error) {
	const node = "admin-demo-seed"
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal([]crdt.ChangeRecord{{
		Table: table, PK: docID, Field: field, CRDTType: crdt.TypeLWW,
		HLC: crdt.HLC{Timestamp: hlcWall, NodeID: node}, NodeID: node, Value: raw,
	}})
}

// seedDemoDoc idempotently seeds one demo CRDT document ("page/welcome") for
// tenant tid by appending two LWW field updates (title, body) through the
// document store, then verifies the merged Snapshot reflects them.
//
// Idempotency: it first probes Snapshot; if the merged state already carries
// the seeded title it returns early without re-applying. Re-applying would be
// harmless anyway (LWW with a fixed HLC converges to the same value), but the
// probe keeps the append log from growing one entry per boot.
//
// Returns true when the document is present and materialized in-CRDT (the seed
// applied or was already there), false only on a hard failure (returned err).
func seedDemoDoc(ctx context.Context, f *fabriq.Fabriq, tid string) (bool, error) {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return false, err
	}
	docID := pageEntity + "/" + demoPageID
	store := f.Document()

	// Idempotency probe: a from-scratch Snapshot of an already-seeded doc carries
	// the title field. A first-boot doc has no updates yet and Snapshot returns an
	// empty field map (no error), so this distinguishes "already seeded" cleanly.
	mat, snapErr := store.Snapshot(tctx, docID)
	if snapErr != nil {
		return false, fmt.Errorf("snapshot probe %s: %w", docID, snapErr)
	}
	if hasField(mat.Snapshot, "title") {
		return true, nil // already seeded
	}

	title, terr := crdtLWWUpdate(pageSpec().Schema.Table, docID, "title", "Welcome to fabriq", 1_000)
	if terr != nil {
		return false, terr
	}
	if aerr := store.ApplyUpdate(tctx, docID, title); aerr != nil {
		return false, fmt.Errorf("apply title update: %w", aerr)
	}
	body, berr := crdtLWWUpdate(pageSpec().Schema.Table, docID, "body",
		"This page is a live CRDT document. Edits merge field-by-field via grove-crdt.", 1_001)
	if berr != nil {
		return false, berr
	}
	if aerr := store.ApplyUpdate(tctx, docID, body); aerr != nil {
		return false, fmt.Errorf("apply body update: %w", aerr)
	}

	// Verify the merge: a fresh Snapshot must now reflect both fields.
	verify, verr := store.Snapshot(tctx, docID)
	if verr != nil {
		return false, fmt.Errorf("verify snapshot %s: %w", docID, verr)
	}
	if !hasField(verify.Snapshot, "title") || !hasField(verify.Snapshot, "body") {
		return false, fmt.Errorf("seed %s did not materialize: snapshot=%s", docID, string(verify.Snapshot))
	}
	return true, nil
}

// hasField reports whether the materialized snapshot JSON (a column-keyed
// object) carries a non-empty value for field.
func hasField(snapshot json.RawMessage, field string) bool {
	if len(snapshot) == 0 {
		return false
	}
	var m map[string]any
	if err := json.Unmarshal(snapshot, &m); err != nil {
		return false
	}
	v, ok := m[field]
	return ok && v != nil
}
