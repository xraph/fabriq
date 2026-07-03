package shard

import (
	"context"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
)

// These thin types implement the source-of-truth ports by routing each call
// to the ctx tenant's shard, then delegating unchanged. They add no
// behaviour beyond the route — the resolved shard's adapter enforces tenant
// scoping, transactions and validation exactly as before.

// Store routes command.Store (the write path) by tenant.
type Store struct{ set Router }

// Relational routes query.RelationalQuerier (reads of the source of truth).
type Relational struct{ set Router }

// Vector routes query.VectorQuerier (embedding upsert/search).
type Vector struct{ set Router }

// Timeseries routes query.TSQuerier (telemetry ingest/range reads).
type Timeseries struct{ set Router }

// Spatial routes query.SpatialQuerier (geometry upsert/search/delete).
type Spatial struct{ set Router }

var (
	_ command.Store           = (*Store)(nil)
	_ query.RelationalQuerier = (*Relational)(nil)
	_ query.VectorQuerier     = (*Vector)(nil)
	_ query.TSQuerier         = (*Timeseries)(nil)
	_ query.SpatialQuerier    = (*Spatial)(nil)
)

// NewStore wraps a Set as a routing command.Store.
func NewStore(set Router) *Store { return &Store{set: set} }

// NewRelational wraps a Set as a routing query.RelationalQuerier.
func NewRelational(set Router) *Relational { return &Relational{set: set} }

// NewVector wraps a Set as a routing query.VectorQuerier.
func NewVector(set Router) *Vector { return &Vector{set: set} }

// NewTimeseries wraps a Set as a routing query.TSQuerier.
func NewTimeseries(set Router) *Timeseries { return &Timeseries{set: set} }

// NewSpatial wraps a Set as a routing query.SpatialQuerier.
func NewSpatial(set Router) *Spatial { return &Spatial{set: set} }

// InTenantTx routes the write transaction to the tenant's shard. The whole
// transaction (aggregate row + event + outbox) runs there, so atomicity is
// preserved without any distributed transaction.
func (s *Store) InTenantTx(ctx context.Context, fn func(ctx context.Context, tx command.Tx) error) error {
	sh, release, err := s.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Store.InTenantTx(ctx, fn)
}

// Get routes a single-row read.
func (r *Relational) Get(ctx context.Context, entity, id string, into any) error {
	sh, release, err := r.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Relational.Get(ctx, entity, id, into)
}

// GetMany routes a batched read (the hydration primitive).
func (r *Relational) GetMany(ctx context.Context, entity string, ids []string, into any) error {
	sh, release, err := r.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Relational.GetMany(ctx, entity, ids, into)
}

// List routes a structured query.
func (r *Relational) List(ctx context.Context, entity string, q query.ListQuery, into any) error {
	sh, release, err := r.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Relational.List(ctx, entity, q, into)
}

// Query routes the raw-SQL escape hatch.
func (r *Relational) Query(ctx context.Context, into any, sql string, args ...any) error {
	sh, release, err := r.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Relational.Query(ctx, into, sql, args...)
}

// Upsert routes an embedding write.
func (v *Vector) Upsert(ctx context.Context, entity, id string, embedding []float32, meta map[string]any) error {
	sh, release, err := v.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Vector.Upsert(ctx, entity, id, embedding, meta)
}

// Similar routes a nearest-neighbour search.
func (v *Vector) Similar(ctx context.Context, q query.VectorQuery, into any) error {
	sh, release, err := v.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Vector.Similar(ctx, q, into)
}

// Delete routes an embedding delete to the tenant's shard.
func (v *Vector) Delete(ctx context.Context, entity, id string) error {
	sh, release, err := v.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Vector.Delete(ctx, entity, id)
}

// Get routes an embedding lookup to the tenant's shard.
func (v *Vector) Get(ctx context.Context, entity, id string) ([]float32, error) {
	sh, release, err := v.set.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return sh.Vector.Get(ctx, entity, id)
}

// DeleteByMeta routes a metadata-filtered delete to the tenant's shard.
func (v *Vector) DeleteByMeta(ctx context.Context, entity string, filter map[string]string) error {
	sh, release, err := v.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Vector.DeleteByMeta(ctx, entity, filter)
}

// BulkWrite routes a telemetry batch.
func (t *Timeseries) BulkWrite(ctx context.Context, series string, points []query.Point) error {
	sh, release, err := t.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Timeseries.BulkWrite(ctx, series, points)
}

// Range routes a time-window read.
func (t *Timeseries) Range(ctx context.Context, q query.RangeQuery, into any) error {
	sh, release, err := t.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Timeseries.Range(ctx, q, into)
}

// Upsert routes a geometry write to the tenant's shard.
func (s *Spatial) Upsert(ctx context.Context, entity, id string, geom query.Geometry, meta map[string]any) error {
	sh, release, err := s.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Spatial.Upsert(ctx, entity, id, geom, meta)
}

// Within routes a radius/nearest search to the tenant's shard.
func (s *Spatial) Within(ctx context.Context, q query.SpatialQuery, into any) error {
	sh, release, err := s.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Spatial.Within(ctx, q, into)
}

// Get routes a single-geometry read to the tenant's shard.
func (s *Spatial) Get(ctx context.Context, entity, id string) (geom query.Geometry, meta map[string]any, ok bool, err error) {
	sh, release, ferr := s.set.Acquire(ctx)
	if ferr != nil {
		return query.Geometry{}, nil, false, ferr
	}
	defer release()
	return sh.Spatial.Get(ctx, entity, id)
}

// Delete routes a geometry delete to the tenant's shard.
func (s *Spatial) Delete(ctx context.Context, entity, id string) error {
	sh, release, err := s.set.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return sh.Spatial.Delete(ctx, entity, id)
}
