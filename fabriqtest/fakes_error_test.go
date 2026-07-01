package fabriqtest_test

import (
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestFakeRelational_UnknownEntityIsInvalidInput confirms the in-memory fake
// classifies an unknown entity name the same way the real postgres adapter
// does: a structured *fabriqerr.Error with CodeInvalidInput, not a plain
// fmt.Errorf. This keeps conformance gating identical across fake and real
// adapters (see core/fabriqerr and the postgres adapter's translatePg path).
func TestFakeRelational_UnknownEntityIsInvalidInput(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	ctx := ftCtx(t, "acme")

	var rows []map[string]any
	err := w.Rel.List(ctx, "products", query.ListQuery{}, &rows)

	var fe *fabriqerr.Error
	if !errors.As(err, &fe) || fe.Code != fabriqerr.CodeInvalidInput {
		t.Fatalf("List: want invalid_input, got %T %v", err, err)
	}
}

// TestFakeRelational_UnknownEntityIsInvalidInput_Get exercises the same
// classification via Get, which shares the fake's entity() lookup helper.
func TestFakeRelational_UnknownEntityIsInvalidInput_Get(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	ctx := ftCtx(t, "acme")

	var into map[string]any
	err := w.Rel.Get(ctx, "products", "1", &into)

	var fe *fabriqerr.Error
	if !errors.As(err, &fe) || fe.Code != fabriqerr.CodeInvalidInput {
		t.Fatalf("Get: want invalid_input, got %T %v", err, err)
	}
}

// TestFakeRelational_MissingRowIsNotFound confirms the fake's existing
// miss path (known entity, absent row) still returns *fabriqerr.NotFoundError,
// unchanged by this task.
func TestFakeRelational_MissingRowIsNotFound(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	ctx := ftCtx(t, "acme")

	var into ftSite
	err := w.Rel.Get(ctx, "site", "does-not-exist", &into)

	var nf *fabriqerr.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Get: want *fabriqerr.NotFoundError, got %T %v", err, err)
	}
}
