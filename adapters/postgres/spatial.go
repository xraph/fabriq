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

var (
	_ query.SpatialQuerier = (*SpatialAdapter)(nil)
	_ query.SpatialCoverer = (*SpatialAdapter)(nil)
)

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
		const sql = `INSERT INTO fabriq_geometries (tenant_id, entity, id, geom, srid, meta, scope_id)
			VALUES ($1, $2, $3, ST_GeomFromText($4, $5), $5, $6, NULLIF($7, ''))
			ON CONFLICT (tenant_id, entity, id)
			DO UPDATE SET geom = EXCLUDED.geom, srid = EXCLUDED.srid, meta = EXCLUDED.meta, scope_id = EXCLUDED.scope_id`
		if _, err := tx.NewRaw(sql, tid, entity, id, geom.WKT, geom.SRID, metaJSON, tenant.ScopeOrEmpty(ctx)).Exec(ctx); err != nil {
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

// Get implements query.SpatialQuerier: fetch the stored geometry (as WKT) and
// meta for (tenant, entity, id). ok=false (nil error) when the id has no row.
func (s *SpatialAdapter) Get(ctx context.Context, entity, id string) (geom query.Geometry, meta map[string]any, ok bool, err error) {
	if _, rerr := tenant.Require(ctx); rerr != nil {
		return query.Geometry{}, nil, false, rerr
	}
	err = s.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		const sql = `SELECT ST_AsText(geom) AS wkt, srid, meta::text AS meta
			FROM fabriq_geometries WHERE tenant_id=$1 AND entity=$2 AND id=$3`
		var rows []struct {
			WKT  string `grove:"wkt"`
			SRID int    `grove:"srid"`
			Meta string `grove:"meta"`
		}
		if serr := tx.NewRaw(sql, tid, entity, id).Scan(ctx, &rows); serr != nil {
			return fmt.Errorf("fabriq: get geometry %s/%s: %w", entity, id, serr)
		}
		if len(rows) == 0 {
			return nil
		}
		ok = true
		geom = query.Geometry{WKT: rows[0].WKT, SRID: rows[0].SRID}
		if rows[0].Meta != "" && rows[0].Meta != "{}" {
			if uerr := json.Unmarshal([]byte(rows[0].Meta), &meta); uerr != nil {
				return uerr
			}
		}
		return nil
	})
	if err != nil {
		return query.Geometry{}, nil, false, err
	}
	return geom, meta, ok, nil
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
	// ST_DWithin (GiST-accelerated for planar geometry) bounds the candidate set;
	// ordering by the computed dist guarantees nearest-first matches the reported
	// metric. For SRID 4326 dist is geography metres; otherwise planar metres in
	// the geometry's own units. (Ordering by dist rather than the KNN operator
	// `<->` avoids degree-vs-metre disagreement across latitudes for 4326.)
	distExpr := "ST_Distance(geom, c)"
	dwithin := "ST_DWithin(geom, c, $5)"
	if q.Center.SRID == 4326 {
		distExpr = "ST_Distance(geom::geography, c::geography)"
		dwithin = "ST_DWithin(geom::geography, c::geography, $5)"
	}
	args := []any{q.Center.WKT, q.Center.SRID, "", q.Entity, q.RadiusM, k}
	filterClause := ""
	if len(q.Filter) > 0 {
		fj, err := json.Marshal(q.Filter)
		if err != nil {
			return err
		}
		filterClause = " AND meta @> $7::jsonb"
		args = append(args, string(fj))
	}
	sql := fmt.Sprintf(`SELECT id, %s AS dist, meta::text AS meta
		FROM fabriq_geometries, (SELECT ST_GeomFromText($1, $2) AS c) cc
		WHERE tenant_id = $3 AND entity = $4
		  AND %s%s
		ORDER BY dist ASC
		LIMIT $6`, distExpr, dwithin, filterClause)
	return s.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		args[2] = tid
		var rows []geoRow
		if err := tx.NewRaw(sql, args...).Scan(ctx, &rows); err != nil {
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

// coverPredicateSQL maps a CoverPredicate to the PostGIS function applied as
// f(geom, probe). All three are GiST-accelerated by fabriq_geometries_gist.
func coverPredicateSQL(p query.CoverPredicate) (string, error) {
	switch p {
	case "", query.CoverContains:
		return "ST_Contains", nil
	case query.CoverWithin:
		return "ST_Within", nil
	case query.CoverIntersects:
		return "ST_Intersects", nil
	default:
		return "", fmt.Errorf("fabriq: unknown cover predicate %q", p)
	}
}

// Covering implements query.SpatialCoverer: a GiST-accelerated topological
// containment search (no radius). Containment is boolean, so DistanceM is 0 and
// results are unordered beyond the K cap. SRID mixing is the caller's
// responsibility — the probe and stored geometry must share an SRID for PostGIS
// to compare them (unlike Within, no geography cast is applied).
func (s *SpatialAdapter) Covering(ctx context.Context, q query.CoverQuery, into any) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	dest, ok := into.(*[]query.SpatialMatch)
	if !ok {
		return fmt.Errorf("fabriq: Covering scans into *[]query.SpatialMatch, got %T", into)
	}
	if q.Probe.WKT == "" {
		return fmt.Errorf("fabriq: Covering requires a probe geometry")
	}
	fn, err := coverPredicateSQL(q.Predicate)
	if err != nil {
		return err
	}
	k := q.K
	if k <= 0 {
		k = 50
	}
	args := []any{q.Probe.WKT, q.Probe.SRID, "", q.Entity, k}
	filterClause := ""
	if len(q.Filter) > 0 {
		fj, ferr := json.Marshal(q.Filter)
		if ferr != nil {
			return ferr
		}
		filterClause = " AND meta @> $6::jsonb"
		args = append(args, string(fj))
	}
	// dist is a constant 0 — containment is boolean, not ranked — but the scan
	// target reuses geoRow so the column list mirrors Within.
	sql := fmt.Sprintf(`SELECT id, 0 AS dist, meta::text AS meta
		FROM fabriq_geometries, (SELECT ST_GeomFromText($1, $2) AS p) pp
		WHERE tenant_id = $3 AND entity = $4
		  AND %s(geom, p)%s
		LIMIT $5`, fn, filterClause)
	return s.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		args[2] = tid
		var rows []geoRow
		if serr := tx.NewRaw(sql, args...).Scan(ctx, &rows); serr != nil {
			return fmt.Errorf("fabriq: covering %s: %w", q.Entity, serr)
		}
		for _, r := range rows {
			m := query.SpatialMatch{ID: r.ID, DistanceM: r.Dist}
			if r.Meta != "" && r.Meta != "{}" {
				if uerr := json.Unmarshal([]byte(r.Meta), &m.Meta); uerr != nil {
					return uerr
				}
			}
			*dest = append(*dest, m)
		}
		return nil
	})
}
