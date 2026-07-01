package fabriqerr

import (
	"errors"
	"fmt"
	"testing"
)

func TestError_MessageIsDriverFree(t *testing.T) {
	cause := errors.New(`pgdriver: tx query: ERROR: relation "products" does not exist (SQLSTATE 42P01)`)
	e := Wrap(CodeSchemaMismatch, cause, SafeMessage(CodeSchemaMismatch),
		WithEntity("products", ""), WithOp("list"))

	if got := e.Error(); got != "fabriq: schema_mismatch: The requested entity type is not available." {
		t.Fatalf("Error() = %q", got)
	}
	if errStr := e.Error(); contains(errStr, "pgdriver") || contains(errStr, "SQLSTATE") {
		t.Fatalf("Error() leaked driver text: %q", errStr)
	}
}

func TestError_UnwrapReachesCause(t *testing.T) {
	cause := errors.New("boom")
	e := Wrap(CodeInternal, cause, "x")
	if !errors.Is(e, cause) {
		t.Fatal("Unwrap must expose the wrapped cause")
	}
}

func TestError_IsMapsCodeToSentinels(t *testing.T) {
	if !errors.Is(New(CodeNotFound, "m"), ErrNotFound) {
		t.Fatal("CodeNotFound must satisfy errors.Is(err, ErrNotFound)")
	}
	if !errors.Is(New(CodeVersionConflict, "m"), ErrVersionConflict) {
		t.Fatal("CodeVersionConflict must satisfy errors.Is(err, ErrVersionConflict)")
	}
	if !errors.Is(New(CodeNotConfigured, "m"), ErrStoreNotConfigured) {
		t.Fatal("CodeNotConfigured must satisfy errors.Is(err, ErrStoreNotConfigured)")
	}
	// A non-matching code must NOT satisfy an unrelated sentinel.
	if errors.Is(New(CodeInternal, "m"), ErrNotFound) {
		t.Fatal("CodeInternal must not match ErrNotFound")
	}
	// Wrapped structured error still matches.
	wrapped := fmt.Errorf("ctx: %w", New(CodeNotFound, "m"))
	if !errors.Is(wrapped, ErrNotFound) {
		t.Fatal("wrapped structured error must still match sentinel")
	}
	var fe *Error
	if !errors.As(wrapped, &fe) || fe.Code != CodeNotFound {
		t.Fatalf("errors.As must recover *Error with Code, got %+v", fe)
	}
}

func TestOptions_SetFields(t *testing.T) {
	e := New(CodeConstraintViolation, "m",
		WithEntity("asset", "01H"), WithOp("insert"), WithRetryable(true),
		WithMeta(Meta{Driver: "postgres", SQLState: "23505"}))
	if e.Entity != "asset" || e.ID != "01H" || e.Op != "insert" || !e.Retryable ||
		e.Meta.SQLState != "23505" {
		t.Fatalf("options not applied: %+v", e)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
