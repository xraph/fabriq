package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// SpatialAdapter wraps Adapter to implement query.SpatialQuerier.
// A separate type is required because *Adapter already carries Upsert for
// query.VectorQuerier ([]float32 embedding) — Go does not allow two methods
// with the same name on one type, so the spatial variant lives here.
type SpatialAdapter struct {
	a *Adapter
}

var _ query.SpatialQuerier = (*SpatialAdapter)(nil)

// NewSpatialAdapter wraps an existing Postgres adapter for geometry operations.
func NewSpatialAdapter(a *Adapter) *SpatialAdapter { return &SpatialAdapter{a: a} }

// Upsert implements query.SpatialQuerier: store/replace a geometry from WKT+SRID.
func (s *SpatialAdapter) Upsert(ctx context.Context, entity, id string, geom query.Geometry, meta map[string]any) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	if geom.WKT == "" {
		return fmt.Errorf("fabriq: empty geometry WKT")
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if meta == nil {
		metaJSON = []byte(`{}`)
	}
	return s.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		const sql = `INSERT INTO fabriq_geometries (tenant_id, entity, id, geom, srid, meta)
			VALUES ($1, $2, $3, ST_GeomFromText($4, $5), $5, $6)
			ON CONFLICT (tenant_id, entity, id)
			DO UPDATE SET geom = EXCLUDED.geom, srid = EXCLUDED.srid, meta = EXCLUDED.meta`
		if _, err := tx.NewRaw(sql, tid, entity, id, geom.WKT, geom.SRID, metaJSON).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: upsert geometry %s/%s: %w", entity, id, err)
		}
		return nil
	})
}

// Delete implements query.SpatialQuerier.
func (s *SpatialAdapter) Delete(ctx context.Context, entity, id string) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	return s.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		const sql = `DELETE FROM fabriq_geometries WHERE tenant_id=$1 AND entity=$2 AND id=$3`
		if _, err := tx.NewRaw(sql, tid, entity, id).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: delete geometry %s/%s: %w", entity, id, err)
		}
		return nil
	})
}

// geoRow is the scan target for Within.
type geoRow struct {
	ID   string  `grove:"id"`
	Dist float64 `grove:"dist"`
	Meta string  `grove:"meta"`
}

// Within implements query.SpatialQuerier: GiST-accelerated radius search,
// nearest-first. For SRID 4326 distance/predicate use the geography cast (true
// metres); otherwise planar metres in the geometry's own units.
func (s *SpatialAdapter) Within(ctx context.Context, q query.SpatialQuery, into any) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	dest, ok := into.(*[]query.SpatialMatch)
	if !ok {
		return fmt.Errorf("fabriq: Within scans into *[]query.SpatialMatch, got %T", into)
	}
	k := q.K
	if k <= 0 {
		k = 50
	}
	var sql string
	if q.Center.SRID == 4326 {
		sql = `SELECT id, ST_Distance(geom::geography, c::geography) AS dist, meta::text AS meta
			FROM fabriq_geometries, (SELECT ST_GeomFromText($1, $2) AS c) cc
			WHERE tenant_id = $3 AND entity = $4
			  AND ST_DWithin(geom::geography, c::geography, $5)
			ORDER BY geom <-> c ASC
			LIMIT $6`
	} else {
		sql = `SELECT id, ST_Distance(geom, c) AS dist, meta::text AS meta
			FROM fabriq_geometries, (SELECT ST_GeomFromText($1, $2) AS c) cc
			WHERE tenant_id = $3 AND entity = $4
			  AND ST_DWithin(geom, c, $5)
			ORDER BY geom <-> c ASC
			LIMIT $6`
	}
	return s.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		var rows []geoRow
		if err := tx.NewRaw(sql, q.Center.WKT, q.Center.SRID, tid, q.Entity, q.RadiusM, k).Scan(ctx, &rows); err != nil {
			return fmt.Errorf("fabriq: within %s: %w", q.Entity, err)
		}
		for _, r := range rows {
			m := query.SpatialMatch{ID: r.ID, DistanceM: r.Dist}
			if r.Meta != "" && r.Meta != "{}" {
				if err := json.Unmarshal([]byte(r.Meta), &m.Meta); err != nil {
					return err
				}
			}
			*dest = append(*dest, m)
		}
		return nil
	})
}
