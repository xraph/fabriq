package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xraph/grove/driver"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// LiveStore implements livequery.Snapshotter and livequery.Refiller over
// Postgres via the existing tenant-stamped read path. Ordering and the keyset
// boundary are Postgres-authoritative — this is the exact-top-N oracle. Reads
// run inside an RLS-scoped transaction (set_config app.tenant_id), so the app
// role only ever sees its own tenant's rows.
type LiveStore struct {
	a *Adapter
}

// NewLiveStore returns the live query snapshot/refill store for this adapter.
func (a *Adapter) NewLiveStore() *LiveStore { return &LiveStore{a: a} }

var (
	_ livequery.Snapshotter  = (*LiveStore)(nil)
	_ livequery.Refiller     = (*LiveStore)(nil)
	_ livequery.MemberLister = (*LiveStore)(nil)
)

// Members returns every aggregate id currently matching the query's filter
// (tenant-scoped, RLS-enforced) — the membership seed for a Streamed
// subscription. No ordering or payloads; just ids.
func (s *LiveStore) Members(ctx context.Context, q livequery.LiveQuery) ([]string, error) {
	ent, err := s.a.entity(q.Entity)
	if err != nil {
		return nil, err
	}
	if err := query.ValidateConds(q.Where, ent.Binding.HasColumn); err != nil {
		return nil, err
	}
	if !ddlValid(ent.Binding.Table) {
		return nil, fmt.Errorf("fabriq: live: table %q failed ddl validation", ent.Binding.Table)
	}
	var ids []string
	err = s.a.inDynamicTenantTx(ctx, func(tid string, tx driver.Tx) error {
		var sb strings.Builder
		var args []any
		argN := 1
		fmt.Fprintf(&sb, `SELECT %s FROM %s WHERE %s = $%d`,
			quoteIdent(registry.ColumnID), quoteIdent(ent.Binding.Table), quoteIdent(registry.ColumnTenant), argN)
		args = append(args, tid)
		argN++
		for _, c := range q.Where {
			frag, fargs, cerr := condSQLPositional(c, &argN)
			if cerr != nil {
				return cerr
			}
			sb.WriteString(" AND ")
			sb.WriteString(frag)
			args = append(args, fargs...)
		}
		drows, qerr := tx.Query(ctx, sb.String(), args...)
		if qerr != nil {
			return qerr
		}
		maps, serr := scanMaps(drows)
		if serr != nil {
			return serr
		}
		ids = make([]string, 0, len(maps))
		for _, m := range maps {
			if idv, ok := m[registry.ColumnID].(string); ok {
				ids = append(ids, idv)
			}
		}
		return nil
	})
	return ids, err
}

// Snapshot returns the first `limit` rows from the anchor in total order.
func (s *LiveStore) Snapshot(ctx context.Context, q livequery.LiveQuery, limit int) ([]livequery.Row, error) {
	return s.read(ctx, q, nil, limit)
}

// After returns up to `limit` rows strictly after `after` in total order —
// the bounded keyset boundary refill.
func (s *LiveStore) After(ctx context.Context, q livequery.LiveQuery, after livequery.Cursor, limit int) ([]livequery.Row, error) {
	return s.read(ctx, q, &after, limit)
}

func (s *LiveStore) read(ctx context.Context, q livequery.LiveQuery, after *livequery.Cursor, limit int) ([]livequery.Row, error) {
	ent, err := s.a.entity(q.Entity)
	if err != nil {
		return nil, err
	}
	// Column validation is the injection guard: every filter column must exist.
	if err := query.ValidateConds(q.Where, ent.Binding.HasColumn); err != nil {
		return nil, err
	}
	if !ddlValid(ent.Binding.Table) {
		return nil, fmt.Errorf("fabriq: live: table %q failed ddl validation", ent.Binding.Table)
	}
	for _, sk := range q.Sort {
		if !ent.Binding.HasColumn(sk.Column) || !ddlValid(sk.Column) {
			return nil, fmt.Errorf("fabriq: live: invalid sort column %q", sk.Column)
		}
	}

	var rows []livequery.Row
	err = s.a.inDynamicTenantTx(ctx, func(tid string, tx driver.Tx) error {
		var sb strings.Builder
		var args []any
		argN := 1

		fmt.Fprintf(&sb, `SELECT * FROM %s WHERE %s = $%d`,
			quoteIdent(ent.Binding.Table), quoteIdent(registry.ColumnTenant), argN)
		args = append(args, tid)
		argN++

		for _, c := range q.Where {
			frag, fargs, cerr := condSQLPositional(c, &argN)
			if cerr != nil {
				return cerr
			}
			sb.WriteString(" AND ")
			sb.WriteString(frag)
			args = append(args, fargs...)
		}
		if after != nil {
			frag, fargs := keysetSQL(q.Sort, *after, &argN)
			sb.WriteString(" AND ")
			sb.WriteString(frag)
			args = append(args, fargs...)
		}

		sb.WriteString(" ORDER BY ")
		for _, sk := range q.Sort {
			dir := "ASC"
			if sk.Desc {
				dir = "DESC"
			}
			fmt.Fprintf(&sb, "%s %s, ", quoteIdent(sk.Column), dir)
		}
		fmt.Fprintf(&sb, "%s ASC", quoteIdent(registry.ColumnID))
		if limit > 0 {
			fmt.Fprintf(&sb, " LIMIT %d", limit)
		}

		drows, qerr := tx.Query(ctx, sb.String(), args...)
		if qerr != nil {
			return qerr
		}
		maps, serr := scanMaps(drows)
		if serr != nil {
			return serr
		}
		rows = make([]livequery.Row, 0, len(maps))
		for _, m := range maps {
			r, cerr := mapToRow(m, q.Sort)
			if cerr != nil {
				return cerr
			}
			rows = append(rows, r)
		}
		return nil
	})
	return rows, err
}

// keysetSQL builds the "strictly after cursor" predicate as the OR-expansion of
// AND-chains so that mixed ASC/DESC sort keys are honored (Postgres row-value
// comparison only supports a single direction). For sort keys s0..s(k-1) plus
// id, with values v0..vk:
//
//	(s0 ▷ v0) OR (s0 = v0 AND s1 ▷ v1) OR … OR (… AND id > vk)
//
// where ▷ is ">" for ascending keys and "<" for descending. Identifiers are
// ddlValid-checked and quoted; every value travels as a bound parameter.
func keysetSQL(sort []livequery.SortKey, after livequery.Cursor, argN *int) (string, []any) {
	cols := make([]string, 0, len(sort)+1)
	desc := make([]bool, 0, len(sort)+1)
	for _, sk := range sort {
		cols = append(cols, sk.Column)
		desc = append(desc, sk.Desc)
	}
	cols = append(cols, registry.ColumnID)
	desc = append(desc, false)

	ph := func() int { n := *argN; *argN++; return n }

	var chains []string
	var args []any
	for i := range cols {
		var parts []string
		for j := 0; j < i; j++ {
			parts = append(parts, fmt.Sprintf("%s = $%d", quoteIdent(cols[j]), ph()))
			args = append(args, after.Values[j])
		}
		op := ">"
		if desc[i] {
			op = "<"
		}
		parts = append(parts, fmt.Sprintf("%s %s $%d", quoteIdent(cols[i]), op, ph()))
		args = append(args, after.Values[i])
		chains = append(chains, "("+strings.Join(parts, " AND ")+")")
	}
	return "(" + strings.Join(chains, " OR ") + ")", args
}

// mapToRow normalizes a scanned column map through JSON so snapshot rows carry
// the same value types as live event payloads (JSON numbers as float64, etc.),
// keeping the in-engine comparison consistent across both sources.
func mapToRow(m map[string]any, sort []livequery.SortKey) (livequery.Row, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return livequery.Row{}, fmt.Errorf("fabriq: live: marshal row: %w", err)
	}
	var vals map[string]any
	if err := json.Unmarshal(raw, &vals); err != nil {
		return livequery.Row{}, fmt.Errorf("fabriq: live: normalize row: %w", err)
	}
	id, _ := vals[registry.ColumnID].(string)
	var ver int64
	if v, ok := vals[registry.ColumnVersion].(float64); ok {
		ver = int64(v)
	}
	return livequery.Row{
		AggID:   id,
		Version: ver,
		Cursor:  livequery.SortKeyOf(vals, sort, id),
		Raw:     raw,
		Vals:    vals,
	}, nil
}
