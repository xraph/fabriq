package projection

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/tenant"
)

// Snapshotter streams a tenant's aggregates as synthetic envelopes at
// their current version (implemented by adapters/postgres — rebuilds
// always replay FROM POSTGRES, never from another projection).
type Snapshotter interface {
	SnapshotEntities(ctx context.Context, tenantID string, fn func(env event.Envelope) error) error
}

// TargetSink is a Sink whose targets can also be dropped (rebuild
// cleanup).
type TargetSink interface {
	Sink
	DropTarget(ctx context.Context, target string) error
}

// Rebuilder performs blue-green projection rebuilds:
//
//  1. Mark projection_state status=building. From this moment the live
//     engine dual-applies every event to the live AND building targets
//     (Engine.TargetsFor) — the live catch-up.
//  2. Replay the Postgres snapshot into the building target. Version
//     gating makes the overlap with live applies safe in both orders.
//  3. Flip: model_version++, target_name=building target, status=soaking.
//     Readers resolve targets through projection_state, so the flip is
//     atomic for them.
//  4. Finalize (after soak): drop the old target, status=live.
type Rebuilder struct {
	Projection string // "graph" | "search"
	State      StateRepo
	Sink       TargetSink
	Applier    Applier
	Snapshot   Snapshotter
	// TargetName derives the versioned build target (registry naming).
	TargetName func(tenantID string, modelVersion int) string
	// OnFlip runs right after the state pointer flips — the seam for
	// engine-side cutovers that must accompany it (Elasticsearch swaps
	// the tenant aliases here, atomically, in one _aliases call).
	OnFlip func(ctx context.Context, tenantID string, oldModelVersion, newModelVersion int) error
}

// Rebuild builds and flips; it returns the old and new target names (the
// old one is dropped by Finalize after the soak window, or immediately if
// the operator passes --drop-old).
func (r *Rebuilder) Rebuild(ctx context.Context, tenantID string) (oldTarget, newTarget string, err error) {
	if r.State == nil || r.Sink == nil || r.Applier == nil || r.Snapshot == nil || r.TargetName == nil {
		return "", "", fmt.Errorf("fabriq: rebuilder %q not fully wired", r.Projection)
	}
	st, err := r.State.Get(ctx, tenantID, r.Projection)
	if err != nil {
		return "", "", err
	}
	if st.Status == "building" {
		return "", "", fmt.Errorf("fabriq: rebuild already in progress for %s/%s", tenantID, r.Projection)
	}
	oldTarget = st.TargetName
	buildVersion := st.ModelVersion + 1
	newTarget = r.TargetName(tenantID, buildVersion)

	// Building marker first: the live engine starts dual-applying NOW, so
	// everything after the snapshot watermark reaches the new target.
	st.Status = "building"
	if upErr := r.State.Upsert(ctx, st); upErr != nil {
		return "", "", upErr
	}
	revert := func() {
		st.Status = "live"
		_ = r.State.Upsert(context.WithoutCancel(ctx), st)
	}

	tctx, err := tenant.WithTenant(ctx, tenantID)
	if err != nil {
		revert()
		return "", "", err
	}

	// A previous abandoned attempt may have left a partial target behind.
	if dropErr := r.Sink.DropTarget(tctx, newTarget); dropErr != nil {
		revert()
		return "", "", dropErr
	}

	// Snapshot replay. Payloads are current-shape: no upcasters here.
	err = r.Snapshot.SnapshotEntities(ctx, tenantID, func(env event.Envelope) error {
		muts, aerr := r.Applier.Apply(env)
		if aerr != nil || len(muts) == 0 {
			return aerr
		}
		return r.Sink.ApplyMutations(tctx, newTarget, muts)
	})
	if err != nil {
		revert()
		return "", "", fmt.Errorf("fabriq: rebuild %s/%s: %w", tenantID, r.Projection, err)
	}

	// Flip the pointer: readers follow projection_state atomically.
	oldModel := buildVersion - 1
	st.ModelVersion = buildVersion
	st.TargetName = newTarget
	st.Status = "soaking"
	if err := r.State.Upsert(ctx, st); err != nil {
		return "", "", err
	}
	if r.OnFlip != nil {
		if err := r.OnFlip(ctx, tenantID, oldModel, buildVersion); err != nil {
			return "", "", fmt.Errorf("fabriq: flip cutover %s/%s: %w", tenantID, r.Projection, err)
		}
	}
	return oldTarget, newTarget, nil
}

// Finalize ends the soak: drops the old target and marks the projection
// live. oldTarget may be empty (first rebuild: the unversioned default
// target was the implicit live one — pass it explicitly to drop it).
func (r *Rebuilder) Finalize(ctx context.Context, tenantID, oldTarget string) error {
	st, err := r.State.Get(ctx, tenantID, r.Projection)
	if err != nil {
		return err
	}
	if oldTarget != "" && oldTarget != st.TargetName {
		tctx, err := tenant.WithTenant(ctx, tenantID)
		if err != nil {
			return err
		}
		if err := r.Sink.DropTarget(tctx, oldTarget); err != nil {
			return err
		}
	}
	st.Status = "live"
	return r.State.Upsert(ctx, st)
}
