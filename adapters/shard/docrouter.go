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

func (d *Documents) docs(ctx context.Context) (document.Store, func(), error) {
	sh, release, err := d.set.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	if sh.Documents == nil {
		release()
		return nil, nil, fabriqerr.New(fabriqerr.CodeNotConfigured,
			"shard has no document plane.")
	}
	return sh.Documents, release, nil
}

// ApplyUpdate implements document.Store.
func (d *Documents) ApplyUpdate(ctx context.Context, docID string, update []byte) error {
	docs, release, err := d.docs(ctx)
	if err != nil {
		return err
	}
	defer release()
	return docs.ApplyUpdate(ctx, docID, update)
}

// ApplyUpdateWithSeq routes the seq-returning apply (the live fan-out
// decorator needs the log seq for gap detection).
func (d *Documents) ApplyUpdateWithSeq(ctx context.Context, docID string, update []byte) (int64, error) {
	docs, release, err := d.docs(ctx)
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
	return seqApplier.ApplyUpdateWithSeq(ctx, docID, update)
}

// Sync implements document.Store.
func (d *Documents) Sync(ctx context.Context, docID string, stateVector []byte) ([]byte, error) {
	docs, release, err := d.docs(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return docs.Sync(ctx, docID, stateVector)
}

// Snapshot implements document.Store.
func (d *Documents) Snapshot(ctx context.Context, docID string) (document.Materialized, error) {
	docs, release, err := d.docs(ctx)
	if err != nil {
		return document.Materialized{}, err
	}
	defer release()
	return docs.Snapshot(ctx, docID)
}

// Compact implements document.Store.
func (d *Documents) Compact(ctx context.Context, docID string) error {
	docs, release, err := d.docs(ctx)
	if err != nil {
		return err
	}
	defer release()
	return docs.Compact(ctx, docID)
}
