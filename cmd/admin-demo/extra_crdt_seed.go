package main

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// noteEntity is a second demo KindDocument (CRDT) entity, distinct from "page",
// so the Documents viewer has more than one doc TYPE to browse. Registering it
// lets the postgres document store accept "note/<id>" document ids.
const noteEntity = "note"

// noteSpec returns the demo KindDocument entity spec for "note". It mirrors
// pageSpec (a CRDTSpec is mandatory for KindDocument) with its own physical
// table so a materialized note row can render in the entity browser too.
func noteSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: noteEntity,
		Kind: registry.KindDocument,
		CRDT: &registry.CRDTSpec{
			Engine:        "grove-crdt",
			SnapshotEvery: 64,
			QuietWindow:   2_000_000_000, // 2s, matching the "page" entity
		},
		Schema: &registry.DynamicSchema{
			Table: "ds_notes",
			Columns: []registry.DynamicColumn{
				{Name: "title", Type: registry.ColText},
				{Name: "body", Type: registry.ColText},
			},
		},
	}
}

// crdtDoc describes one extra CRDT document to seed: its entity type (page or
// note), id, table (the entity's DynamicSchema table), and the title/body field
// values. The seed appends two LWW updates (title, body) through the document
// store, exactly like seedDemoDoc does for page/welcome.
type crdtDoc struct {
	entity string
	table  string
	id     string
	title  string
	body   string
}

// extraCRDTDocs is the per-tenant set of additional documents seeded alongside
// the original page/welcome: a second page ("about") and a note ("roadmap").
// Bodies differ slightly per tenant so tenant isolation is visible.
func extraCRDTDocs(tid string) []crdtDoc {
	return []crdtDoc{
		{
			entity: pageEntity,
			table:  pageSpec().Schema.Table,
			id:     "about",
			title:  "About this workspace",
			body:   "This is the " + tid + " workspace. The About page is a live CRDT document — edits merge field-by-field via grove-crdt.",
		},
		{
			entity: noteEntity,
			table:  noteSpec().Schema.Table,
			id:     "roadmap",
			title:  "Roadmap",
			body:   "Roadmap for " + tid + ": 1) richer entities, 2) cross-type graph, 3) semantic search over every type.",
		},
	}
}

// seedExtraCRDTDocs idempotently seeds the extraCRDTDocs for tenant tid via the
// DIRECT document-store write path (ApplyUpdate with LWW change records),
// verifying each via Snapshot. Idempotency mirrors seedDemoDoc: it probes
// Snapshot per doc and skips when the title field is already materialized.
// Returns the number of docs that are present after this run.
func seedExtraCRDTDocs(ctx context.Context, f *fabriq.Fabriq, tid string) (int, error) {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return 0, err
	}
	store := f.Document()

	seeded := 0
	for _, d := range extraCRDTDocs(tid) {
		docID := d.entity + "/" + d.id

		mat, snapErr := store.Snapshot(tctx, docID)
		if snapErr != nil {
			return seeded, fmt.Errorf("snapshot probe %s: %w", docID, snapErr)
		}
		if hasField(mat.Snapshot, "title") {
			seeded++
			continue // already seeded
		}

		const node = "admin-demo-seed"
		title, terr := crdtLWWUpdate(d.table, docID, "title", d.title, 2_000, node)
		if terr != nil {
			return seeded, terr
		}
		if aerr := store.ApplyUpdate(tctx, docID, title); aerr != nil {
			return seeded, fmt.Errorf("apply title update %s: %w", docID, aerr)
		}
		body, berr := crdtLWWUpdate(d.table, docID, "body", d.body, 2_001, node)
		if berr != nil {
			return seeded, berr
		}
		if aerr := store.ApplyUpdate(tctx, docID, body); aerr != nil {
			return seeded, fmt.Errorf("apply body update %s: %w", docID, aerr)
		}

		verify, verr := store.Snapshot(tctx, docID)
		if verr != nil {
			return seeded, fmt.Errorf("verify snapshot %s: %w", docID, verr)
		}
		if !hasField(verify.Snapshot, "title") || !hasField(verify.Snapshot, "body") {
			return seeded, fmt.Errorf("seed %s did not materialize: snapshot=%s", docID, string(verify.Snapshot))
		}
		seeded++
	}
	return seeded, nil
}
