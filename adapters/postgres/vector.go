package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

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
		const sql = `INSERT INTO fabriq_embeddings (tenant_id, entity, id, embedding, meta)
			VALUES ($1, $2, $3, $4::vector, $5)
			ON CONFLICT (tenant_id, entity, id)
			DO UPDATE SET embedding = EXCLUDED.embedding, meta = EXCLUDED.meta`
		if _, err := tx.NewRaw(sql, tid, entity, id, vectorLiteral(embedding), metaJSON).Exec(ctx); err != nil {
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
		var rows []vectorRow
		const sql = `SELECT id, 1 - (embedding <=> $1::vector) AS score, meta::text AS meta
			FROM fabriq_embeddings
			WHERE tenant_id = $2 AND entity = $3
			ORDER BY embedding <=> $1::vector ASC
			LIMIT $4`
		if err := tx.NewRaw(sql, vectorLiteral(q.Embedding), tid, q.Entity, k).Scan(ctx, &rows); err != nil {
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
