// Package document is the CRDT document plane for KindDocument entities
// (collaborative documents: page-builder documents, annotations).
//
// THIS PASS DEFINES THE SEAM ONLY. The port below and the crdt_updates /
// crdt_snapshots migrations exist from phase 1 so the contract is stable;
// sync transport and materialization are deferred (see DESIGN.md).
//
// Intended semantics (normative for the future implementation):
//
//   - Updates are appended to the crdt_updates table (append-only, per-doc
//     monotonic sequence). The merge engine comes from grove's crdt /
//     crdt-js packages — referenced, never reimplemented here.
//   - Bidirectional sync rides the subscription hub's connection layer
//     WITHOUT conflation and WITHOUT update coalescing (Hub.PublishRaw is
//     the seam) — CRDT updates must arrive complete and in order.
//   - After a per-document quiet window, materialization snapshots the
//     merged state into the entity's relational row and emits ONE ordinary
//     domain event (<entity>.updated, version++) through the outbox, so
//     projections, search and audit see CRDT documents as normal entities.
//   - Materialization runs post-merge validation: CRDTs converge but do
//     not guarantee business validity; violations flag the document for
//     resolution instead of silently materializing.
//   - Awareness/presence (cursors, who's online) is ephemeral Redis
//     pub/sub, never persisted.
package document

import (
	"context"
	"encoding/json"
	"time"
)

// Materialized is a point-in-time snapshot of a document's merged state.
type Materialized struct {
	DocID    string
	Snapshot json.RawMessage
	// Version is the aggregate version assigned by the LAST materialization
	// event; it advances only when the quiet-window snapshot lands.
	Version int64
}

// Store is the document-plane port. The production implementation (phase 7)
// lives in adapters/postgres backed by crdt_updates + crdt_snapshots;
// fabriqtest provides a fake that returns ErrStoreNotConfigured-style
// errors until then.
type Store interface {
	// ApplyUpdate appends one encoded CRDT update to the document's log.
	ApplyUpdate(ctx context.Context, docID string, update []byte) error

	// Sync returns the updates the caller is missing, given its encoded
	// state vector (the CRDT engine's diff protocol).
	Sync(ctx context.Context, docID string, stateVector []byte) ([]byte, error)

	// Snapshot returns the current merged state of the document.
	Snapshot(ctx context.Context, docID string) (Materialized, error)

	// Compact folds the update log into a snapshot row and trims the log
	// (SnapshotEvery from the entity's CRDTSpec governs cadence).
	Compact(ctx context.Context, docID string) error
}

// HistoryUpdate is one raw update from the offloaded log, keyed by its seq.
// Update is the verbatim JSON-encoded []crdt.ChangeRecord.
type HistoryUpdate struct {
	Seq    int64           `json:"seq"`
	Update json.RawMessage `json:"update"`
}

// HistoryReader is an optional capability on document stores that offload
// history to the blob plane: it reconstructs a raw update range from sealed
// file segments plus any still-in-DB rows. Stores that do not offload need
// not implement it; consumers type-assert (like core/blob's Presigner/Ranger).
type HistoryReader interface {
	// ReadHistory returns every update with seqLo <= seq <= seqHi, in seq
	// order, drawn from sealed segments and the live update log.
	ReadHistory(ctx context.Context, docID string, seqLo, seqHi int64) ([]HistoryUpdate, error)
}

// SegmentInfo is the metadata for one sealed history segment (a contiguous
// [SeqLo, SeqHi] range of the update log stored as a blob). Blob keys are an
// internal storage detail and are intentionally not exposed.
type SegmentInfo struct {
	SegSeq      int64     `json:"segSeq"`
	SeqLo       int64     `json:"seqLo"`
	SeqHi       int64     `json:"seqHi"`
	UpdateCount int64     `json:"updateCount"`
	ByteSize    int64     `json:"byteSize"`
	At          time.Time `json:"at"`
}

// SegmentLister is an optional capability on document stores that offload
// history to the blob plane: it lists a document's sealed segments (newest
// storage-shape metadata, not the update bytes). Consumers type-assert for it.
type SegmentLister interface {
	ListSegments(ctx context.Context, docID string) ([]SegmentInfo, error)
}

// HistoryPurger is an optional capability: delete a document's offloaded
// history (segment blobs + index rows). Consumers type-assert for it (e.g. the
// admin delete path purges history when a document entity is deleted).
type HistoryPurger interface {
	DeleteHistory(ctx context.Context, docID string) error
}
