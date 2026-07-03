// Package shard routes a tenant's source-of-truth operations to the
// Postgres shard that holds it. A tenant lives entirely on one shard
// (ADR 0007), so routing is a directory lookup, never a distributed
// transaction or a scatter-gather read.
//
// The router is engine-neutral: it speaks only the fabriq capability ports
// (command.Store, query.RelationalQuerier/VectorQuerier/TSQuerier/SpatialQuerier) and
// implements those same ports by delegating to the resolved shard. The
// facade, executor and every call site are therefore unchanged — sharding
// is one more adapter behind a port. Single-Postgres deployments use the
// degenerate one-shard Set (Single), for which routing is a no-op.
package shard

import (
	"context"
	"fmt"
	"sort"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/sweep"
	"github.com/xraph/fabriq/core/tenant"
)

// Shard is one tenant-home: the source-of-truth ports for the tenants that
// live on it. In production a single Postgres adapter satisfies all five;
// tests supply stubs.
type Shard struct {
	ID         string
	Store      command.Store
	Relational query.RelationalQuerier
	Vector     query.VectorQuerier
	Timeseries query.TSQuerier
	Spatial    query.SpatialQuerier
	// Documents is the shard's CRDT document plane. Static deployments
	// leave it nil (the primary's doc store serves, ADR 0007 step 2);
	// catalog mode fills it per tenant database.
	Documents document.Store
	// Maintenance is the shard's single-pass worker surface for the
	// catalog-mode sweeper. Static deployments leave it nil (the worker
	// plane runs its own boot-time loops).
	Maintenance sweep.Maintainer
	// Projection is the shard's projection bookkeeping surface
	// (projection_applied/_state stay co-located with the aggregates they
	// track). Static deployments leave it nil (the worker plane holds
	// concrete adapters); catalog mode fills it per tenant database.
	Projection ProjectionStateStore
}

// ProjectionStateStore is the per-shard projection bookkeeping the engines,
// rebuilder and WaitForProjection route to (satisfied by the Postgres
// adapter's StateRepo).
type ProjectionStateStore interface {
	projection.StateRepo
	// SetApplied records the applied version (projection.AppliedRecorder).
	SetApplied(ctx context.Context, tenantID, proj, aggregate, aggID string, version int64) error
	// Tenants lists every tenant this shard has bookkeeping for.
	Tenants(ctx context.Context) ([]string, error)
}

// Directory resolves a tenant to its shard id. Implementations range from a
// constant (one shard) to a cached, versioned table (many shards) — the
// router does not care which.
type Directory interface {
	Shard(ctx context.Context, tenantID string) (string, error)
}

// Set is the tenant -> shard routing table: a directory plus the shards it
// can name.
type Set struct {
	shards map[string]Shard
	dir    Directory
}

// New builds a routing Set from a directory and its shards. Every shard
// needs a non-empty, unique id; the set needs at least one.
func New(dir Directory, shards ...Shard) (*Set, error) {
	if dir == nil {
		return nil, fmt.Errorf("fabriq: shard set needs a directory")
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("fabriq: shard set needs at least one shard")
	}
	m := make(map[string]Shard, len(shards))
	for _, sh := range shards {
		if sh.ID == "" {
			return nil, fmt.Errorf("fabriq: shard with empty id")
		}
		if _, dup := m[sh.ID]; dup {
			return nil, fmt.Errorf("fabriq: duplicate shard id %q", sh.ID)
		}
		m[sh.ID] = sh
	}
	return &Set{shards: m, dir: dir}, nil
}

// Single builds the degenerate one-shard Set: every tenant routes to sh.
// This is what single-Postgres deployments use, so the routing layer is a
// no-op until a second shard is configured. An empty sh.ID defaults to "0".
func Single(sh Shard) *Set {
	if sh.ID == "" {
		sh.ID = "0"
	}
	return &Set{shards: map[string]Shard{sh.ID: sh}, dir: constDirectory(sh.ID)}
}

// For resolves the shard owning the ctx tenant. It requires a tenant (the
// same precondition every source-of-truth port already enforces) and fails
// loudly if the directory names a shard the set does not hold.
func (s *Set) For(ctx context.Context) (Shard, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return Shard{}, err
	}
	return s.ForTenant(ctx, tid)
}

// ForTenant resolves the shard owning an explicit tenant id — the seam the
// worker plane uses to route by the tenant carried in a method argument
// (projection bookkeeping, snapshot, reconcile) rather than on ctx.
func (s *Set) ForTenant(ctx context.Context, tenantID string) (Shard, error) {
	id, err := s.dir.Shard(ctx, tenantID)
	if err != nil {
		return Shard{}, err
	}
	sh, ok := s.shards[id]
	if !ok {
		return Shard{}, fmt.Errorf("fabriq: tenant %q routed to unknown shard %q", tenantID, id)
	}
	return sh, nil
}

// ResolveID returns just the shard id a tenant routes to — for the worker
// plane to pick the matching concrete adapter (relay, reconcile, snapshot)
// from its own per-shard map.
func (s *Set) ResolveID(ctx context.Context, tenantID string) (string, error) {
	return s.dir.Shard(ctx, tenantID)
}

// IDs returns the shard ids, sorted — for building per-shard worker runners.
func (s *Set) IDs() []string {
	out := make([]string, 0, len(s.shards))
	for id := range s.shards {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// All returns the shards, ordered by id — for the worker plane to start a
// per-shard relay / reconciler against each.
func (s *Set) All() []Shard {
	out := make([]Shard, 0, len(s.shards))
	for _, sh := range s.shards {
		out = append(out, sh)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Len reports the number of shards.
func (s *Set) Len() int { return len(s.shards) }

// constDirectory routes every tenant to one shard id.
type constDirectory string

func (c constDirectory) Shard(context.Context, string) (string, error) {
	return string(c), nil
}
