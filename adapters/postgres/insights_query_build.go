package postgres

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/query"
)

// insightsIdentRe is the injection guard for every JSONB prop key (dimension,
// measure field) and every output alias (measure As, OrderBy column) that
// buildInsightsSQL interpolates into the emitted SQL. These are the ONLY
// non-bound tokens in the query; everything else — tenant, source, filter
// values, time-bucket interval, from/to — travels as a $N parameter. A key
// or alias that fails this check is rejected outright, never sanitized.
var insightsIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// propAccessor renders a JSONB prop key as `<jsonCol> ->> 'key'` after
// checking it against insightsIdentRe. Every dimension and every non-count
// measure field goes through this before appearing in SQL. jsonCol is
// "props" for events, "payload" for facts — a resolver-produced constant,
// never user input, so it is interpolated directly.
func propAccessor(jsonCol, key string) (string, error) {
	if !insightsIdentRe.MatchString(key) {
		return "", fmt.Errorf("fabriq: invalid insights key %q", key)
	}
	return fmt.Sprintf("%s ->> '%s'", jsonCol, key), nil
}

// isNumericValue reports whether v is a Go numeric type, as opposed to a
// string or other kind, so mapCondToProp knows whether a Gt/Gte/Lt/Lte bound
// warrants a numeric cast on its JSONB accessor. Mirrors the int/uint/float
// family fabriqtest's toFloat (fabriqtest/filter.go) coerces — the same
// numeric-detection the in-memory fake's evalConds relies on for these ops —
// plus json.Number for condition values that arrived via a JSON round-trip
// (e.g. core/insights/conformance.go's toFloatT handles the same case for
// decoded query results). adapters/postgres does not import fabriqtest or
// core/insights (both pull in "testing"), so the switch is duplicated here
// rather than shared.
func isNumericValue(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		json.Number:
		return true
	default:
		return false
	}
}

// measureAlias computes the (possibly defaulted) output column name for a
// measure and checks it against insightsIdentRe. Shared by measureExpr
// (which quotes it into the SELECT list) and buildInsightsSQL (which adds it
// to the set of columns OrderBy may reference).
func measureAlias(m query.Measure) (string, error) {
	alias := m.As
	if alias == "" {
		switch {
		case m.Kind == query.MeasureCount:
			alias = "count"
		case m.Kind == query.MeasurePercentile:
			alias = fmt.Sprintf("p%d_%s", int(math.Round(m.Percentile*100)), m.Field)
		default:
			alias = string(m.Kind) + "_" + m.Field
		}
	}
	if !insightsIdentRe.MatchString(alias) {
		return "", fmt.Errorf("fabriq: invalid measure alias %q", alias)
	}
	return alias, nil
}

// measureAggExpr renders one Measure as a BARE aggregate expression (no
// "AS alias" suffix) plus its alias, separately, so a caller building a
// HAVING clause can repeat the aggregate expression verbatim — Postgres
// cannot reference a SELECT-list alias from HAVING (unlike GROUP BY/ORDER
// BY). jsonCol is the JSONB column to accessor into ("props" for events,
// "payload" for facts). allowed, when non-nil, is the resolver's column
// allow-list for this source (facts only); a measure field not in it is
// rejected. argN is the next free positional-parameter index; a measure
// that needs to bind a value (only MeasurePercentile, for its fraction)
// consumes it via *argN and returns the bound value(s) in extraArgs for the
// caller to append to args. measureExpr composes aggExpr and alias into the
// full "<aggExpr> AS <alias>" SELECT-list entry.
func measureAggExpr(m query.Measure, jsonCol string, allowed map[string]bool, argN *int) (aggExpr, alias string, extraArgs []any, err error) {
	alias, err = measureAlias(m)
	if err != nil {
		return "", "", nil, err
	}
	if m.Kind == query.MeasureCount {
		return "COUNT(*)", alias, nil, nil
	}
	if m.Kind == query.MeasurePercentile {
		if !(m.Percentile > 0 && m.Percentile < 1) {
			return "", "", nil, fmt.Errorf("fabriq: percentile must be in (0,1), got %v", m.Percentile)
		}
		acc, err := propAccessor(jsonCol, m.Field)
		if err != nil {
			return "", "", nil, err
		}
		if allowed != nil && !allowed[m.Field] {
			return "", "", nil, fmt.Errorf("fabriq: insights column %q is not declared for this source", m.Field)
		}
		p := fmt.Sprintf("$%d", *argN)
		*argN++
		return fmt.Sprintf("percentile_cont(%s) WITHIN GROUP (ORDER BY (%s)::numeric)", p, acc), alias, []any{m.Percentile}, nil
	}
	acc, err := propAccessor(jsonCol, m.Field)
	if err != nil {
		return "", "", nil, err
	}
	if allowed != nil && !allowed[m.Field] {
		return "", "", nil, fmt.Errorf("fabriq: insights column %q is not declared for this source", m.Field)
	}
	num := "(" + acc + ")::numeric"
	var fn string
	switch m.Kind {
	case query.MeasureSum:
		fn = "SUM(" + num + ")"
	case query.MeasureAvg:
		fn = "AVG(" + num + ")"
	case query.MeasureMin:
		fn = "MIN(" + num + ")"
	case query.MeasureMax:
		fn = "MAX(" + num + ")"
	case query.MeasureCountDistinct:
		fn = "COUNT(DISTINCT " + acc + ")"
	default:
		return "", "", nil, fmt.Errorf("fabriq: unknown measure kind %q", m.Kind)
	}
	return fn, alias, nil, nil
}

// measureExpr renders one Measure as a full SELECT-list aggregate
// expression ("<aggExpr> AS <alias>"). See measureAggExpr for the parameter
// contract; this is a thin composer over it for callers that only need the
// SELECT-list form.
func measureExpr(m query.Measure, jsonCol string, allowed map[string]bool, argN *int) (expr string, extraArgs []any, err error) {
	aggExpr, alias, extraArgs, err := measureAggExpr(m, jsonCol, allowed, argN)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%s AS %q", aggExpr, alias), extraArgs, nil
}

// mapCondToProp renders one filter condition against a JSONB prop, mirroring
// condSQLPositional's operator handling (adapters/postgres/filter.go) but
// with the column rewritten to its `props ->> 'key'` accessor instead of a
// quoted table column.
//
// This deliberately does NOT reuse condSQLPositional by pre-rewriting
// c.Column to the accessor string and passing it through: condSQLPositional
// quotes its column via quoteIdent, which wraps the ENTIRE string in double
// quotes and strips embedded `"` — given a multi-token expression like
// `props ->> 'status'` that produces a single malformed (and semantically
// wrong) identifier, not valid SQL. Re-implementing the same op switch here
// with propAccessor in place of quoteIdent keeps the same injection guard —
// column via an identifier-checked accessor, value always via a bound $N
// placeholder — while emitting correct SQL for the JSONB case.
func mapCondToProp(c query.Cond, argN *int, jsonCol string, allowed map[string]bool) (frag string, args []any, err error) {
	if c.IsGroup() {
		if len(c.Or) == 0 {
			return "", nil, fmt.Errorf("fabriq: empty OR group in insights filter")
		}
		var parts []string
		for _, sub := range c.Or {
			f, a, serr := mapCondToProp(sub, argN, jsonCol, allowed)
			if serr != nil {
				return "", nil, serr
			}
			parts = append(parts, f)
			args = append(args, a...)
		}
		return "(" + strings.Join(parts, " OR ") + ")", args, nil
	}

	acc, err := propAccessor(jsonCol, c.Column)
	if err != nil {
		return "", nil, err
	}
	if allowed != nil && !allowed[c.Column] {
		return "", nil, fmt.Errorf("fabriq: insights column %q is not declared for this source", c.Column)
	}
	ph := func() string {
		p := fmt.Sprintf("$%d", *argN)
		*argN++
		return p
	}
	switch c.Op {
	case query.OpGt, query.OpGte, query.OpLt, query.OpLte:
		// Range comparisons over a JSONB prop default to a TEXT comparison
		// (props ->> 'col' is already ::text), which is lexicographic — "50"
		// > "100" as strings — and silently returns wrong rows for numeric
		// data. Measures on the same field already cast to numeric
		// (measureExpr); mirror that here whenever the bound value itself is
		// numeric, so `Gt("amount", 100)` compares numerically. A
		// string-valued bound (e.g. comparing a status/version string field)
		// keeps the plain text comparison.
		col := acc
		if isNumericValue(c.Value) {
			col = "(" + acc + ")::numeric"
		}
		return fmt.Sprintf("%s %s %s", col, sqlOp[c.Op], ph()), []any{c.Value}, nil
	case query.OpEq, query.OpNe, query.OpLike, query.OpILike:
		return fmt.Sprintf("%s %s %s", acc, sqlOp[c.Op], ph()), []any{c.Value}, nil
	case query.OpIn:
		return fmt.Sprintf("%s = ANY(%s)", acc, ph()), []any{c.Value}, nil
	case query.OpNotIn:
		return fmt.Sprintf("NOT (%s = ANY(%s))", acc, ph()), []any{c.Value}, nil
	case query.OpIsNull:
		return fmt.Sprintf("%s IS NULL", acc), nil, nil
	case query.OpIsNotNull:
		return fmt.Sprintf("%s IS NOT NULL", acc), nil, nil
	default:
		return "", nil, fmt.Errorf("fabriq: unsupported filter operator %q", c.Op)
	}
}

// mapHavingCond renders one post-aggregation filter condition (an
// AnalyticsQuery.Having entry) against aliasExpr — the measure-alias ->
// bare-aggregate-expression map buildInsightsSQL assembles while emitting
// the SELECT list. Postgres cannot reference a SELECT-list alias from
// HAVING (unlike GROUP BY/ORDER BY), so the aggregate expression is
// repeated verbatim here instead of the alias. Looking c.Column up in
// aliasExpr is BOTH the validation (Having may only reference a measure
// this query actually selects) AND the injection guard: c.Column is a
// caller-supplied string, and only a name that is already a map key —
// i.e. one that passed through measureAlias's insightsIdentRe check when
// the measure was emitted — is ever interpolated; c.Column itself never
// reaches the SQL string.
//
// Percentile aggregate expressions embed their fraction as a "$N"
// placeholder that was already bound into args when the measure was
// emitted (measureAggExpr). Repeating that same aggExpr string here
// re-references the SAME positional parameter — Postgres permits a
// placeholder to appear more than once in a query, and it is exactly
// correct to do so since it is the same value — so Having over a
// percentile alias needs no extra binding for the fraction, only a fresh
// $N for the cond's own comparison value (via argN/ph below).
func mapHavingCond(c query.Cond, aliasExpr map[string]string, argN *int) (frag string, args []any, err error) {
	if c.IsGroup() {
		if len(c.Or) == 0 {
			return "", nil, fmt.Errorf("fabriq: empty OR group in insights having")
		}
		var parts []string
		for _, sub := range c.Or {
			f, a, serr := mapHavingCond(sub, aliasExpr, argN)
			if serr != nil {
				return "", nil, serr
			}
			parts = append(parts, f)
			args = append(args, a...)
		}
		return "(" + strings.Join(parts, " OR ") + ")", args, nil
	}

	aggExpr, ok := aliasExpr[c.Column]
	if !ok {
		return "", nil, fmt.Errorf("fabriq: having references unknown measure alias %q", c.Column)
	}
	ph := func() string {
		p := fmt.Sprintf("$%d", *argN)
		*argN++
		return p
	}
	switch c.Op {
	case query.OpGt, query.OpGte, query.OpLt, query.OpLte, query.OpEq, query.OpNe, query.OpLike, query.OpILike:
		return fmt.Sprintf("%s %s %s", aggExpr, sqlOp[c.Op], ph()), []any{c.Value}, nil
	case query.OpIn:
		return fmt.Sprintf("%s = ANY(%s)", aggExpr, ph()), []any{c.Value}, nil
	case query.OpNotIn:
		return fmt.Sprintf("NOT (%s = ANY(%s))", aggExpr, ph()), []any{c.Value}, nil
	case query.OpIsNull:
		return fmt.Sprintf("%s IS NULL", aggExpr), nil, nil
	case query.OpIsNotNull:
		return fmt.Sprintf("%s IS NOT NULL", aggExpr), nil, nil
	default:
		return "", nil, fmt.Errorf("fabriq: unsupported having operator %q", c.Op)
	}
}

// buildInsightsOrder validates and renders an OrderBy clause. Each
// comma-separated term is "col" or "col ASC|DESC"; col must be a member of
// allowed — the set of declared dimension names, "bucket" (only when
// time-bucketing), and measure output aliases assembled by
// buildInsightsSQL. Anything else is rejected: this is the injection guard
// for the ORDER BY clause, mirroring propAccessor's role for JSONB keys.
// Members of allowed already passed insightsIdentRe when they were declared,
// so a match here is sufficient to interpolate col safely.
func buildInsightsOrder(orderBy string, allowed map[string]bool) (string, error) {
	terms := strings.Split(orderBy, ",")
	out := make([]string, 0, len(terms))
	for _, t := range terms {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		fields := strings.Fields(t)
		if len(fields) == 0 || len(fields) > 2 {
			return "", fmt.Errorf("fabriq: invalid order by term %q", t)
		}
		col := fields[0]
		if !allowed[col] {
			return "", fmt.Errorf("fabriq: order by references unknown column %q", col)
		}
		dir := ""
		if len(fields) == 2 {
			d := strings.ToUpper(fields[1])
			if d != "ASC" && d != "DESC" {
				return "", fmt.Errorf("fabriq: invalid order by direction %q", fields[1])
			}
			dir = d
		}
		if dir != "" {
			out = append(out, fmt.Sprintf("%q %s", col, dir))
		} else {
			out = append(out, fmt.Sprintf("%q", col))
		}
	}
	if len(out) == 0 {
		return "", fmt.Errorf("fabriq: empty order by")
	}
	return strings.Join(out, ", "), nil
}

// buildInsightsSQL builds the cube aggregation SQL and its positional args
// for one AnalyticsQuery scoped to tenant tid, against the source resolved by
// insights.ResolveSource. $1 = tenant_id, $2 = d.KeyValue (the event name or
// projected entity name); time-window, time-bucket-interval, and filter
// values continue from $3. d.Table, d.KeyColumn, d.JSONColumn, and
// d.ExtraWhere are constants produced by the resolver — never user input —
// so they are interpolated directly; d.KeyValue always travels bound as $2.
// See insightsIdentRe for the injection-guard contract on user-supplied
// identifiers (dimension/measure/filter keys, OrderBy columns).
//
// Having (post-aggregation filtering over measure outputs) is emitted as a
// HAVING clause after GROUP BY; see aliasExpr below and mapHavingCond for
// how a Having condition's alias is resolved back to its aggregate
// expression.
func buildInsightsSQL(q query.AnalyticsQuery, tid string, d insights.Descriptor) (sql string, args []any, err error) {
	if len(q.Measures) == 0 {
		return "", nil, fmt.Errorf("fabriq: insights query needs at least one measure")
	}

	args = []any{tid, d.KeyValue}
	argN := 3

	// allowedOrder collects every output column name OrderBy may reference:
	// declared dimensions, "bucket" (only if time-bucketing), and measure
	// aliases. Built alongside the SELECT list so it can only contain names
	// that already passed insightsIdentRe.
	allowedOrder := map[string]bool{}

	// aliasExpr maps each measure's output alias to its BARE aggregate
	// expression (no "AS alias" suffix), so a Having condition referencing
	// that alias can repeat the aggregate expression in the HAVING clause —
	// Postgres cannot reference a SELECT-list alias from HAVING. See
	// mapHavingCond.
	aliasExpr := map[string]string{}

	var selects, groups []string
	for _, dim := range q.Dimensions {
		acc, err := propAccessor(d.JSONColumn, dim)
		if err != nil {
			return "", nil, err
		}
		if d.AllowedColumns != nil && !d.AllowedColumns[dim] {
			return "", nil, fmt.Errorf("fabriq: insights dimension %q is not declared for this source", dim)
		}
		selects = append(selects, fmt.Sprintf("%s AS %q", acc, dim))
		groups = append(groups, acc)
		allowedOrder[dim] = true
	}
	if q.TimeBucket > 0 {
		// time_bucket takes an interval; bind the duration as a "N seconds"
		// literal rather than interpolating it.
		selects = append(selects, fmt.Sprintf(`time_bucket($%d::interval, at) AS bucket`, argN))
		groups = append(groups, "bucket")
		args = append(args, fmt.Sprintf("%d seconds", int64(q.TimeBucket/time.Second)))
		argN++
		allowedOrder["bucket"] = true
	}
	for _, m := range q.Measures {
		aggExpr, alias, extraArgs, err := measureAggExpr(m, d.JSONColumn, d.AllowedColumns, &argN)
		if err != nil {
			return "", nil, err
		}
		selects = append(selects, fmt.Sprintf("%s AS %q", aggExpr, alias))
		args = append(args, extraArgs...)
		allowedOrder[alias] = true
		aliasExpr[alias] = aggExpr
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "SELECT %s FROM %s WHERE tenant_id = $1 AND %s = $2",
		strings.Join(selects, ", "), d.Table, d.KeyColumn)
	if d.ExtraWhere != "" {
		fmt.Fprintf(&sb, " AND %s", d.ExtraWhere)
	}
	if !q.From.IsZero() {
		fmt.Fprintf(&sb, " AND at >= $%d", argN)
		args = append(args, q.From)
		argN++
	}
	if !q.To.IsZero() {
		fmt.Fprintf(&sb, " AND at < $%d", argN)
		args = append(args, q.To)
		argN++
	}
	for _, c := range q.Filter {
		frag, fargs, cerr := mapCondToProp(c, &argN, d.JSONColumn, d.AllowedColumns)
		if cerr != nil {
			return "", nil, cerr
		}
		sb.WriteString(" AND ")
		sb.WriteString(frag)
		args = append(args, fargs...)
	}
	if len(groups) > 0 {
		fmt.Fprintf(&sb, " GROUP BY %s", strings.Join(groups, ", "))
	}
	if len(q.Having) > 0 {
		var havingParts []string
		for _, c := range q.Having {
			frag, hargs, herr := mapHavingCond(c, aliasExpr, &argN)
			if herr != nil {
				return "", nil, herr
			}
			havingParts = append(havingParts, frag)
			args = append(args, hargs...)
		}
		fmt.Fprintf(&sb, " HAVING %s", strings.Join(havingParts, " AND "))
	}
	if q.OrderBy != "" {
		ord, err := buildInsightsOrder(q.OrderBy, allowedOrder)
		if err != nil {
			return "", nil, err
		}
		fmt.Fprintf(&sb, " ORDER BY %s", ord)
	} else if len(groups) > 0 {
		fmt.Fprintf(&sb, " ORDER BY %s", strings.Join(groups, ", "))
	}
	if q.Limit > 0 {
		fmt.Fprintf(&sb, " LIMIT %d", q.Limit)
	}
	if q.Offset > 0 {
		fmt.Fprintf(&sb, " OFFSET %d", q.Offset)
	}
	return sb.String(), args, nil
}
