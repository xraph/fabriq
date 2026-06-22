package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// VectorAdapter wraps Adapter to implement query.VectorQuerier.
// A separate type is required because *Adapter already carries Get for
// query.RelationalQuerier (entity, id string, into any) — Go does not allow
// two methods with the same name on one type, so the vector variant lives here.
// The existing Upsert/Similar/Delete methods remain on *Adapter for backwards
// compat; VectorAdapter delegates to them.
type VectorAdapter struct {
	a *Adapter
}

var _ query.VectorQuerier = (*VectorAdapter)(nil)

// NewVectorAdapter wraps an existing Postgres adapter for vector operations.
func NewVectorAdapter(a *Adapter) *VectorAdapter { return &VectorAdapter{a: a} }

// vectorLiteral renders a pgvector input literal ("[1,2,3]"). Parameters
// are passed as text and cast — pgx needs no pgvector type registration.
func vectorLiteral(emb []float32) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, v := range emb {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatFloat(float64(v), 'f', -1, 32))
	}
	sb.WriteByte(']')
	return sb.String()
}

// Upsert implements query.VectorQuerier.
func (v *VectorAdapter) Upsert(ctx context.Context, entity, id string, embedding []float32, meta map[string]any) error {
	return v.a.Upsert(ctx, entity, id, embedding, meta)
}

// Similar implements query.VectorQuerier.
func (v *VectorAdapter) Similar(ctx context.Context, q query.VectorQuery, into any) error {
	return v.a.Similar(ctx, q, into)
}

// Delete implements query.VectorQuerier.
func (v *VectorAdapter) Delete(ctx context.Context, entity, id string) error {
	return v.a.Delete(ctx, entity, id)
}

// DeleteByMeta implements query.VectorQuerier.
func (v *VectorAdapter) DeleteByMeta(ctx context.Context, entity string, filter map[string]string) error {
	return v.a.DeleteByMeta(ctx, entity, filter)
}

// Get implements query.VectorQuerier. Returns the stored embedding for
// (entity, id) as []float32, or *fabriqerr.NotFoundError on miss.
func (v *VectorAdapter) Get(ctx context.Context, entity, id string) ([]float32, error) {
	if _, err := tenant.Require(ctx); err != nil {
		return nil, err
	}
	var result []float32
	err := v.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		type embRow struct {
			Embedding string `grove:"embedding"`
		}
		var rows []embRow
		const sql = `SELECT embedding::text AS embedding
			FROM fabriq_embeddings
			WHERE tenant_id = $1 AND entity = $2 AND id = $3
			LIMIT 1`
		if err := tx.NewRaw(sql, tid, entity, id).Scan(ctx, &rows); err != nil {
			return fmt.Errorf("fabriq: get embedding %s/%s: %w", entity, id, err)
		}
		if len(rows) == 0 {
			return &fabriqerr.NotFoundError{Entity: entity, ID: id}
		}
		emb, err := parseVectorLiteral(rows[0].Embedding)
		if err != nil {
			return fmt.Errorf("fabriq: parse embedding %s/%s: %w", entity, id, err)
		}
		result = emb
		return nil
	})
	return result, err
}

// parseVectorLiteral parses a pgvector text literal "[1.0,2.0,3.0]" into
// a []float32. This is the inverse of vectorLiteral.
func parseVectorLiteral(s string) ([]float32, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil, fmt.Errorf("fabriq: malformed vector literal %q", s)
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return []float32{}, nil
	}
	parts := strings.Split(inner, ",")
	out := make([]float32, len(parts))
	for i, p := range parts {
		vf, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil, fmt.Errorf("fabriq: parse vector component %q: %w", p, err)
		}
		out[i] = float32(vf)
	}
	return out, nil
}

// --- methods kept on *Adapter for backward compat (shard.Vector used a directly) ---

// Upsert implements query.VectorQuerier (kept on *Adapter for backward compat).
func (a *Adapter) Upsert(ctx context.Context, entity, id string, embedding []float32, meta map[string]any) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	if len(embedding) == 0 {
		return fmt.Errorf("fabriq: empty embedding")
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if meta == nil {
		metaJSON = []byte(`{}`)
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		const sql = `INSERT INTO fabriq_embeddings (tenant_id, entity, id, embedding, meta, scope_id)
			VALUES ($1, $2, $3, $4::vector, $5, NULLIF($6, ''))
			ON CONFLICT (tenant_id, entity, id)
			DO UPDATE SET embedding = EXCLUDED.embedding, meta = EXCLUDED.meta, scope_id = EXCLUDED.scope_id`
		if _, err := tx.NewRaw(sql, tid, entity, id, vectorLiteral(embedding), metaJSON, tenant.ScopeOrEmpty(ctx)).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: upsert embedding %s/%s: %w", entity, id, err)
		}
		return nil
	})
}

type vectorRow struct {
	ID    string  `grove:"id"`
	Score float64 `grove:"score"`
	Meta  string  `grove:"meta"`
}

// Similar implements query.VectorQuerier: cosine nearest neighbours
// through the HNSW index.
func (a *Adapter) Similar(ctx context.Context, q query.VectorQuery, into any) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	dest, ok := into.(*[]query.VectorMatch)
	if !ok {
		return fmt.Errorf("fabriq: Similar scans into *[]query.VectorMatch, got %T", into)
	}
	k := q.K
	if k <= 0 {
		k = 10
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		filterJSON, ferr := metaFilterJSON(q.Filter)
		if ferr != nil {
			return ferr
		}
		var rows []vectorRow
		const sql = `SELECT id, 1 - (embedding <=> $1::vector) AS score, meta::text AS meta
			FROM fabriq_embeddings
			WHERE tenant_id = $2 AND entity = $3
			  AND ($5::jsonb = '{}'::jsonb OR meta @> $5::jsonb)
			ORDER BY embedding <=> $1::vector ASC
			LIMIT $4`
		if err := tx.NewRaw(sql, vectorLiteral(q.Embedding), tid, q.Entity, k, string(filterJSON)).Scan(ctx, &rows); err != nil {
			return fmt.Errorf("fabriq: similar %s: %w", q.Entity, err)
		}
		for _, r := range rows {
			m := query.VectorMatch{ID: r.ID, Score: r.Score}
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

// Delete implements query.VectorQuerier.
func (a *Adapter) Delete(ctx context.Context, entity, id string) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		const sql = `DELETE FROM fabriq_embeddings WHERE tenant_id=$1 AND entity=$2 AND id=$3`
		if _, err := tx.NewRaw(sql, tid, entity, id).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: delete embedding %s/%s: %w", entity, id, err)
		}
		return nil
	})
}

// DeleteByMeta removes embeddings for (tenant, entity) matching the meta filter.
func (a *Adapter) DeleteByMeta(ctx context.Context, entity string, filter map[string]string) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		filterJSON, ferr := metaFilterJSON(filter)
		if ferr != nil {
			return ferr
		}
		const sql = `DELETE FROM fabriq_embeddings
			WHERE tenant_id=$1 AND entity=$2
			  AND ($3::jsonb = '{}'::jsonb OR meta @> $3::jsonb)`
		if _, err := tx.NewRaw(sql, tid, entity, string(filterJSON)).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: delete-by-meta %s: %w", entity, err)
		}
		return nil
	})
}

// metaFilterJSON renders a metadata filter as a JSON object for `meta @>`
// containment. An empty/nil filter yields "{}" (matches everything). A marshal
// failure is propagated so callers fail closed rather than silently widening.
func metaFilterJSON(filter map[string]string) ([]byte, error) {
	if len(filter) == 0 {
		return []byte("{}"), nil
	}
	b, err := json.Marshal(filter)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// parsePGTime parses Postgres text timestamps in the formats time::text
// produces.
func parsePGTime(s string) (time.Time, error) {
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05-07",
		time.RFC3339Nano,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("fabriq: cannot parse pg timestamp %q", s)
}
