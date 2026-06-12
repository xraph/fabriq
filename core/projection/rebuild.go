package projection

import (
	"context"
	"fmt"
)

// PHASE 4 SCAFFOLD — blue-green projection rebuild.
//
// Rebuild contract (normative; `fabriq rebuild --tenant T --projection P`):
//
//  1. SNAPSHOT WATERMARK: open a repeatable-read Postgres transaction,
//     record the projection group's current stream position W, and stream
//     every aggregate row of the tenant (grove cursor streaming).
//  2. BUILD: synthesize created-events from the snapshot rows and apply
//     them through the SAME applier into the NEW target
//     (tenant_{id}_v{N+1} graph / fabriq_{tenant}_{index}_v{N+1});
//     projection_state row: status=building, target_name=new target,
//     model_version=N+1.
//  3. LIVE CATCH-UP: replay stream entries after W into the new target
//     until lag is under a threshold (the engine applies to both targets
//     while status=building).
//  4. FLIP: update projection_state (status=soaking, target_name=new) —
//     for ES additionally swap the alias atomically in the same step;
//     readers resolve targets through projection_state/alias only.
//  5. SOAK + DROP: after the soak window with no reconciler drift, delete
//     the old target (GRAPH.DELETE / DELETE index); status=live.
//
// Always rebuilt FROM POSTGRES — never from the old projection.
type Rebuilder struct {
	State StateRepo
	Sink  Sink
	// Snapshot streams the tenant's aggregates as synthetic envelopes at
	// the snapshot watermark (implemented on the postgres adapter).
	// TODO(phase 4): define alongside the cursor-streaming support.
}

// Rebuild runs the blue-green rebuild for one tenant+projection.
func (r *Rebuilder) Rebuild(ctx context.Context, tenantID, projection string) error {
	_ = ctx
	_ = tenantID
	_ = projection
	return fmt.Errorf("fabriq: rebuild not implemented yet (phase 4)")
}
