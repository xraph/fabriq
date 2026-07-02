package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xraph/fabriq/core/fabriqerr"
)

func TestTranslatePg_TypedPgError(t *testing.T) {
	pg := &pgconn.PgError{
		Code: "42P01", Severity: "ERROR", TableName: "products",
		Message: `relation "products" does not exist`,
	}
	wrapped := fmt.Errorf("fabriq: list products: %w",
		fmt.Errorf("pgdriver: query: %w", pg)) // mimic grove wrapping

	out := translatePg("list", "products", "", wrapped)

	var fe *fabriqerr.Error
	if !errors.As(out, &fe) {
		t.Fatalf("want *fabriqerr.Error, got %T", out)
	}
	if fe.Code != fabriqerr.CodeSchemaMismatch {
		t.Fatalf("Code = %q, want schema_mismatch", fe.Code)
	}
	if fe.Meta.SQLState != "42P01" || fe.Meta.Table != "products" ||
		fe.Meta.Driver != "postgres" {
		t.Fatalf("Meta not extracted: %+v", fe.Meta)
	}
	if fe.Meta.Detail["driverMessage"] != `relation "products" does not exist` {
		t.Fatalf("driverMessage detail missing: %+v", fe.Meta.Detail)
	}
	if fe.Entity != "products" || fe.Op != "list" {
		t.Fatalf("context not attached: %+v", fe)
	}
	// cause preserved for logs.
	if !errors.Is(out, pg) {
		t.Fatal("cause chain must still reach the pg error")
	}
}

func TestTranslatePg_RegexFallback(t *testing.T) {
	// No typed PgError in the chain — only the string carries the SQLSTATE.
	err := errors.New(`pgdriver: query: ERROR: duplicate key value (SQLSTATE 23505)`)
	out := translatePg("insert", "asset", "01H", err)

	var fe *fabriqerr.Error
	if !errors.As(out, &fe) {
		t.Fatalf("want *fabriqerr.Error, got %T", out)
	}
	if fe.Code != fabriqerr.CodeAlreadyExists || fe.Meta.SQLState != "23505" {
		t.Fatalf("fallback classify wrong: %+v", fe)
	}
}

func TestTranslatePg_RegexFallback_RejectsMalformedToken(t *testing.T) {
	// A 6-alphanumeric token is not a valid SQLSTATE (always exactly 5 chars).
	// No typed pgconn error and no "no rows" — must NOT be classified via the
	// regex fallback, so translatePg leaves the error unchanged.
	err := errors.New(`pgdriver: query: ERROR: something odd (SQLSTATE 235051)`)
	out := translatePg("insert", "asset", "01H", err)

	var fe *fabriqerr.Error
	if errors.As(out, &fe) {
		t.Fatalf("malformed 6-char token must not be classified, got *fabriqerr.Error: %+v", fe)
	}
	if !errors.Is(out, err) {
		t.Fatalf("malformed token must pass through unchanged, got %v", out)
	}
}

func TestTranslatePg_NoRows(t *testing.T) {
	out := translatePg("get", "site", "01J", errors.New("sql: no rows in result set"))
	if !errors.Is(out, fabriqerr.ErrNotFound) {
		t.Fatalf("no-rows must classify as not_found, got %v", out)
	}
}

func TestTranslatePg_PassThrough(t *testing.T) {
	// nil stays nil.
	if translatePg("", "", "", nil) != nil {
		t.Fatal("nil must stay nil")
	}
	// Already-structured errors are returned unchanged.
	existing := fabriqerr.New(fabriqerr.CodeNotFound, "x", fabriqerr.WithEntity("a", "1"))
	if got := translatePg("", "", "", existing); !errors.Is(got, existing) {
		t.Fatal("already-structured error must pass through unchanged")
	}
	// The rich NotFoundError passes through (still matches sentinel).
	nf := &fabriqerr.NotFoundError{Entity: "a", ID: "1"}
	if got := translatePg("", "", "", nf); !errors.Is(got, fabriqerr.ErrNotFound) {
		t.Fatal("NotFoundError must pass through and still match ErrNotFound")
	}
	// A non-driver internal error is left untouched (boundary sanitizes it).
	plain := errors.New("fabriq: some internal invariant broke")
	if got := translatePg("", "", "", plain); !errors.Is(got, plain) {
		t.Fatal("non-driver error must pass through unchanged")
	}
}

func TestClassifySQLState(t *testing.T) {
	cases := map[string]struct {
		code  fabriqerr.Code
		retry bool
	}{
		"42P01": {fabriqerr.CodeSchemaMismatch, false},
		"42703": {fabriqerr.CodeSchemaMismatch, false},
		"23505": {fabriqerr.CodeAlreadyExists, false},
		"23503": {fabriqerr.CodeConstraintViolation, false},
		"23502": {fabriqerr.CodeConstraintViolation, false},
		"40001": {fabriqerr.CodeSerialization, true},
		"40P01": {fabriqerr.CodeSerialization, true},
		"57014": {fabriqerr.CodeTimeout, false},
		"42501": {fabriqerr.CodePermissionDenied, false},
		"08006": {fabriqerr.CodeUnavailable, true},
		"28000": {fabriqerr.CodePermissionDenied, false},
		"XX000": {fabriqerr.CodeInternal, false},
	}
	for state, want := range cases {
		gotCode, gotRetry := classifySQLState(state)
		if gotCode != want.code || gotRetry != want.retry {
			t.Errorf("classifySQLState(%q) = (%q,%v), want (%q,%v)",
				state, gotCode, gotRetry, want.code, want.retry)
		}
	}
}
