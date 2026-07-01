package postgres

import (
	"errors"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xraph/fabriq/core/fabriqerr"
)

var sqlstateRe = regexp.MustCompile(`SQLSTATE (\w{5})`)

// translatePg converts a grove/pgdriver error into a structured *fabriqerr.Error.
// It is the single classification seam for the postgres adapter: applied at the
// transaction chokepoints, it maps driver faults into the shared taxonomy while
// leaving everything else (nil, already-structured errors, fabriq sentinels,
// non-driver internal errors) untouched. Idempotent and nil-safe.
func translatePg(op, entity, id string, err error) error {
	if err == nil {
		return nil
	}
	// Already structured, or a rich fabriqerr type — preserve call-site context.
	var fe *fabriqerr.Error
	if errors.As(err, &fe) {
		return err
	}
	if errors.Is(err, fabriqerr.ErrNotFound) || errors.Is(err, fabriqerr.ErrVersionConflict) {
		return err
	}

	// Preferred path: recover the typed pg error (grove wraps with %w).
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		code, retry := classifySQLState(pg.Code)
		meta := fabriqerr.Meta{
			Driver:     "postgres",
			SQLState:   pg.Code,
			Constraint: pg.ConstraintName,
			Table:      pg.TableName,
			Column:     pg.ColumnName,
			Detail:     pgDetail(pg),
		}
		return fabriqerr.New(code, fabriqerr.SafeMessage(code),
			appendCtx(op, entity, id,
				fabriqerr.WithMeta(meta), fabriqerr.WithRetryable(retry), fabriqerr.WithCause(err))...)
	}

	// Fallback: parse the SQLSTATE token out of the message.
	if m := sqlstateRe.FindStringSubmatch(err.Error()); m != nil {
		code, retry := classifySQLState(m[1])
		meta := fabriqerr.Meta{Driver: "postgres", SQLState: m[1]}
		return fabriqerr.New(code, fabriqerr.SafeMessage(code),
			appendCtx(op, entity, id,
				fabriqerr.WithMeta(meta), fabriqerr.WithRetryable(retry), fabriqerr.WithCause(err))...)
	}

	// no-rows -> not_found (folds the old isNoRows string check).
	if strings.Contains(err.Error(), "no rows") {
		return fabriqerr.New(fabriqerr.CodeNotFound, fabriqerr.SafeMessage(fabriqerr.CodeNotFound),
			appendCtx(op, entity, id, fabriqerr.WithCause(err))...)
	}

	// Not a driver error — leave it. The boundary renderer sanitizes any
	// unstructured error to a generic 500, so nothing leaks.
	return err
}

// appendCtx prepends op/entity context options (when present) to trailing opts.
func appendCtx(op, entity, id string, tail ...fabriqerr.Option) []fabriqerr.Option {
	var head []fabriqerr.Option
	if op != "" {
		head = append(head, fabriqerr.WithOp(op))
	}
	if entity != "" || id != "" {
		head = append(head, fabriqerr.WithEntity(entity, id))
	}
	return append(head, tail...)
}

// classifySQLState maps a PostgreSQL SQLSTATE to a fabriq Code and whether the
// failure is retryable. Reference: PostgreSQL "Appendix A. Error Codes".
func classifySQLState(s string) (fabriqerr.Code, bool) {
	switch s {
	case "42P01", "42703", "42P07", "42704", "3F000", "42883":
		// undefined_table / undefined_column / duplicate_table /
		// undefined_object / invalid_schema_name / undefined_function
		return fabriqerr.CodeSchemaMismatch, false
	case "23505":
		return fabriqerr.CodeAlreadyExists, false
	case "23503", "23514", "23502", "23P01":
		// fk / check / not-null / exclusion violations
		return fabriqerr.CodeConstraintViolation, false
	case "40001", "40P01":
		// serialization_failure / deadlock_detected
		return fabriqerr.CodeSerialization, true
	case "57014":
		return fabriqerr.CodeTimeout, false
	case "42501":
		return fabriqerr.CodePermissionDenied, false
	}
	switch {
	case strings.HasPrefix(s, "08"): // connection exceptions
		return fabriqerr.CodeUnavailable, true
	case strings.HasPrefix(s, "28"): // invalid authorization
		return fabriqerr.CodePermissionDenied, false
	case strings.HasPrefix(s, "53"): // insufficient resources
		return fabriqerr.CodeUnavailable, true
	}
	return fabriqerr.CodeInternal, false
}

// pgDetail collects pgconn's native, non-empty extras. driverMessage carries the
// original driver text — structured here, never in the top-level message.
func pgDetail(pg *pgconn.PgError) map[string]string {
	d := map[string]string{}
	put := func(k, v string) {
		if v != "" {
			d[k] = v
		}
	}
	put("severity", pg.Severity)
	put("hint", pg.Hint)
	put("detail", pg.Detail)
	put("schema", pg.SchemaName)
	put("routine", pg.Routine)
	put("driverMessage", pg.Message)
	if len(d) == 0 {
		return nil
	}
	return d
}
