package fabriq

import (
	"context"
	"fmt"
	"sort"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
)

// This file holds the tenant-routing of the WORKER plane. The facade's
// capability ports route through adapters/shard; the worker plane reads and
// writes Postgres by a tenant carried in a method argument (projection
// bookkeeping, snapshots, reconcile truth/repair), so it routes on that
// argument to the owning shard's concrete adapter. The relay and document
// plane are not here — they are per-shard / primary-shard runners wired in
// the worker, not tenant-routed.

// ShardPG pairs a shard id with its concrete Postgres adapter — what the
// worker iterates to start a per-shard relay.
type ShardPG struct {
	ID string
	PG *postgres.Adapter
}

// ShardPGs returns the source-of-truth shards in id order.
func (s *Stores) ShardPGs() []ShardPG {
	ids := s.Shards.IDs()
	out := make([]ShardPG, 0, len(ids))
	for _, id := range ids {
		out = append(out, ShardPG{ID: id, PG: s.shardPG[id]})
	}
	return out
}

// shardForTenant resolves the concrete adapter that owns a tenant.
func (s *Stores) shardForTenant(ctx context.Context, tenantID string) (*postgres.Adapter, error) {
	id, err := s.Shards.ResolveID(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	pg, ok := s.shardPG[id]
	if !ok {
		return nil, fmt.Errorf("fabriq: tenant %q routed to unknown shard %q", tenantID, id)
	}
	return pg, nil
}

// AllTenants unions every shard's known tenants (the outbox-derived
// discovery the reconciler and rebuild --all-tenants scan). Sorted, deduped.
func (s *Stores) AllTenants(ctx context.Context) ([]string, error) {
	seen := map[string]struct{}{}
	var out []string
	for _, sp := range s.ShardPGs() {
		ts, err := sp.PG.ProjectionState().Tenants(ctx)
		if err != nil {
			return nil, err
		}
		for _, t := range ts {
			if _, ok := seen[t]; !ok {
				seen[t] = struct{}{}
				out = append(out, t)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

// truthVersions is the reconciler's TruthVersions, routed to the tenant's
// shard.
func (s *Stores) truthVersions(ctx context.Context, tenantID, entity string) (map[string]int64, error) {
	pg, err := s.shardForTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return pg.AggregateVersions(ctx, tenantID, entity)
}

// repair is the reconciler's RepairFunc, routed to the tenant's shard (the
// synthetic event lands on that shard's outbox, where its relay republishes
// it).
func (s *Stores) repair(ctx context.Context, tenantID string, d projection.Drift) error {
	pg, err := s.shardForTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	return pg.Repair(ctx, tenantID, d)
}

// routingState is the projection.StateRepo seen by the engines, rebuilder
// and WaitForProjection: each call routes on its tenant argument to the
// owning shard's bookkeeping. Projection_applied/_state stay co-located with
// the aggregates they track, so this write stream shards with the data
// instead of re-centralising on one database.
type routingState struct{ stores *Stores }

var (
	_ projection.StateRepo       = routingState{}
	_ projection.AppliedRecorder = routingState{}
)

func (r routingState) repo(ctx context.Context, tenantID string) (*postgres.StateRepo, error) {
	pg, err := r.stores.shardForTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return pg.ProjectionState(), nil
}

func (r routingState) AppliedVersion(ctx context.Context, tenantID, proj, aggregate, aggID string) (int64, error) {
	repo, err := r.repo(ctx, tenantID)
	if err != nil {
		return 0, err
	}
	return repo.AppliedVersion(ctx, tenantID, proj, aggregate, aggID)
}

func (r routingState) SetApplied(ctx context.Context, tenantID, proj, aggregate, aggID string, version int64) error {
	repo, err := r.repo(ctx, tenantID)
	if err != nil {
		return err
	}
	return repo.SetApplied(ctx, tenantID, proj, aggregate, aggID, version)
}

func (r routingState) Get(ctx context.Context, tenantID, proj string) (projection.State, error) {
	repo, err := r.repo(ctx, tenantID)
	if err != nil {
		return projection.State{}, err
	}
	return repo.Get(ctx, tenantID, proj)
}

func (r routingState) Upsert(ctx context.Context, st projection.State) error {
	repo, err := r.repo(ctx, st.TenantID)
	if err != nil {
		return err
	}
	return repo.Upsert(ctx, st)
}

// routingSnapshot is the projection.Snapshotter seen by the rebuilder: a
// tenant's snapshot replays from the shard that holds its aggregates.
type routingSnapshot struct{ stores *Stores }

var _ projection.Snapshotter = routingSnapshot{}

func (r routingSnapshot) SnapshotEntities(ctx context.Context, tenantID string, fn func(env event.Envelope) error) error {
	pg, err := r.stores.shardForTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	return pg.SnapshotEntities(ctx, tenantID, fn)
}
