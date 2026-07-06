package shard

import (
	"context"

	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// Documents routes document.Store by the ctx tenant — the catalog-mode
// document plane, where each tenant's CRDT tables live in its own
// database (lifting ADR 0007 step 2's primary-only restriction).
type Documents struct{ set Router }

// NewDocuments builds the document-plane router.
func NewDocuments(set Router) *Documents { return &Documents{set: set} }

var _ document.Store = (*Documents)(nil)

// docs resolves the tenant's document store and returns the routing context
// to use with it (schema-per-tenant stamps search_path onto it).
func (d *Documents) docs(ctx context.Context) (document.Store, context.Context, func(), error) {
	sh, sctx, release, err := d.set.Acquire(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if sh.Documents == nil {
		release()
		return nil, nil, nil, fabriqerr.New(fabriqerr.CodeNotConfigured,
			"shard has no document plane.")
	}
	return sh.Documents, sctx, release, nil
}

// ApplyUpdate implements document.Store.
func (d *Documents) ApplyUpdate(ctx context.Context, docID string, update []byte) error {
	docs, sctx, release, err := d.docs(ctx)
	if err != nil {
		return err
	}
	defer release()
	return docs.ApplyUpdate(sctx, docID, update)
}

// ApplyUpdateWithSeq routes the seq-returning apply (the live fan-out
// decorator needs the log seq for gap detection).
func (d *Documents) ApplyUpdateWithSeq(ctx context.Context, docID string, update []byte) (int64, error) {
	docs, sctx, release, err := d.docs(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	seqApplier, ok := docs.(interface {
		ApplyUpdateWithSeq(ctx context.Context, docID string, update []byte) (int64, error)
	})
	if !ok {
		return 0, fabriqerr.New(fabriqerr.CodeNotConfigured,
			"shard document store does not report log seqs.")
	}
	return seqApplier.ApplyUpdateWithSeq(sctx, docID, update)
}

// Sync implements document.Store.
func (d *Documents) Sync(ctx context.Context, docID string, stateVector []byte) ([]byte, error) {
	docs, sctx, release, err := d.docs(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return docs.Sync(sctx, docID, stateVector)
}

// Snapshot implements document.Store.
func (d *Documents) Snapshot(ctx context.Context, docID string) (document.Materialized, error) {
	docs, sctx, release, err := d.docs(ctx)
	if err != nil {
		return document.Materialized{}, err
	}
	defer release()
	return docs.Snapshot(sctx, docID)
}

// Compact implements document.Store.
func (d *Documents) Compact(ctx context.Context, docID string) error {
	docs, sctx, release, err := d.docs(ctx)
	if err != nil {
		return err
	}
	defer release()
	return docs.Compact(sctx, docID)
}
