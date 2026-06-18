package fabriq

import (
	"context"
	"fmt"
	"io"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/domain"
)

// BlobRef identifies a stored blob_object and its content.
type BlobRef struct {
	ID      string `json:"id"`
	Hash    string `json:"hash"`
	Size    int64  `json:"size"`
	Version int64  `json:"version"`
}

// PutBlobOpts carries optional metadata for a blob write.
type PutBlobOpts struct {
	ContentType string `json:"contentType"`
}

// PutBlob stores bytes (content-addressed, deduped) then creates the
// blob_object catalog row — one versioned event. Bytes-first,
// command-authoritative. Returns ErrStoreNotConfigured when CAS is not wired.
func (f *Fabriq) PutBlob(ctx context.Context, r io.Reader, opts PutBlobOpts) (BlobRef, error) {
	if f.ports.CAS == nil {
		return BlobRef{}, fmt.Errorf("fabriq: PutBlob: %w", ErrStoreNotConfigured)
	}
	hash, size, err := f.ports.CAS.Store(ctx, r)
	if err != nil {
		return BlobRef{}, fmt.Errorf("fabriq: PutBlob: store bytes: %w", err)
	}
	res, err := f.exec.Exec(ctx, command.Command{
		Entity:  "blob_object",
		Op:      command.OpCreate,
		Payload: &domain.BlobObject{Hash: hash, Size: size, ContentType: opts.ContentType},
	})
	if err != nil {
		return BlobRef{}, fmt.Errorf("fabriq: PutBlob: create blob_object: %w", err)
	}
	return BlobRef{ID: res.AggID, Hash: hash, Size: size, Version: res.Version}, nil
}

// GetBlob resolves the blob_object catalog row by ID, then streams its bytes
// from CAS. Returns ErrStoreNotConfigured when CAS is not wired.
func (f *Fabriq) GetBlob(ctx context.Context, blobObjectID string) (io.ReadCloser, BlobRef, error) {
	if f.ports.CAS == nil {
		return nil, BlobRef{}, fmt.Errorf("fabriq: GetBlob: %w", ErrStoreNotConfigured)
	}
	var bo domain.BlobObject
	if err := f.Relational().Get(ctx, "blob_object", blobObjectID, &bo); err != nil {
		return nil, BlobRef{}, fmt.Errorf("fabriq: GetBlob: %w", err)
	}
	rc, err := f.ports.CAS.Retrieve(ctx, bo.Hash)
	if err != nil {
		return nil, BlobRef{}, fmt.Errorf("fabriq: GetBlob: retrieve bytes: %w", err)
	}
	return rc, BlobRef{ID: bo.ID, Hash: bo.Hash, Size: bo.Size, Version: bo.Version}, nil
}

// DeleteBlob removes the blob_object catalog row (one versioned event). Byte
// GC and ref-count decrement are deferred to Phase 4.
func (f *Fabriq) DeleteBlob(ctx context.Context, blobObjectID string) error {
	_, err := f.exec.Exec(ctx, command.Command{
		Entity: "blob_object",
		Op:     command.OpDelete,
		AggID:  blobObjectID,
	})
	if err != nil {
		return fmt.Errorf("fabriq: DeleteBlob: %w", err)
	}
	return nil
}
