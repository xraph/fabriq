package command

import (
	"context"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
)

// LifecycleHook observes — and may veto or augment — every committed change.
// It is the in-transaction, write-side, cross-cutting seam (distinct from the
// per-entity EntitySpec.Validate input check and the post-commit projection
// appliers). The canonical use is an auditing/chronicle extension that records
// every change atomically with the write. Mirrors the Applier/ApplierFunc
// pattern used by projections.
type LifecycleHook interface {
	// OnChange runs INSIDE the write transaction, after the aggregate row and
	// its outbox event are staged and before commit. Returning an error aborts
	// the whole command (and any batch it belongs to): the write, the outbox
	// event, and anything the hook wrote via tx all roll back together. Use tx
	// to write additional rows atomically with the change.
	OnChange(ctx context.Context, tx Tx, change Change) error
}

// HookFunc adapts a function to LifecycleHook.
type HookFunc func(ctx context.Context, tx Tx, change Change) error

// OnChange implements LifecycleHook.
func (f HookFunc) OnChange(ctx context.Context, tx Tx, change Change) error {
	return f(ctx, tx, change)
}

// Change is the unit a LifecycleHook receives. Envelope is the canonical
// record — the exact event written to the outbox (tenant, agg id, version,
// type, after-image payload, commit time, traceparent, event id). Entity and
// Op are conveniences so the hook need not re-parse the type string.
type Change struct {
	Entity   *registry.Entity
	Op       Op // resolved write op: OpCreate / OpUpdate / OpDelete
	Envelope event.Envelope
}
