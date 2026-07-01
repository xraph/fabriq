package fabriqtest_test

import (
	"errors"
	"testing"

	"github.com/xraph/fabriq/conformance"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestConformance_Fake is the fast (Docker-free) conformance gate: the fakes
// must satisfy every universal case and correctly degrade on the ones that
// require capabilities they lack. It runs in `make test`.
func TestConformance_Fake(t *testing.T) {
	conformance.RunAll(t, fabriqtest.NewConformanceBackend(t))
}

// TestConformance_UnknownEntityCode proves the fake backend classifies an
// unknown entity as fabriqerr.CodeInvalidInput through the SAME surface the
// conformance suite drives it via — conformance.Backend.Setup's Env.Relational
// and Env.Ctx — rather than reaching into fabriqtest.World directly. This
// guards the ExpectCode hook: a future capability-gated case that sets
// Degrade.ExpectCode: fabriqerr.CodeInvalidInput on an unknown-entity read
// relies on exactly this classification holding for the fake.
func TestConformance_UnknownEntityCode(t *testing.T) {
	b := fabriqtest.NewConformanceBackend(t)
	env := b.Setup(t)

	var rows []map[string]any
	err := env.Relational.List(env.Ctx, "no_such_entity", query.ListQuery{}, &rows)

	var fe *fabriqerr.Error
	if !errors.As(err, &fe) || fe.Code != fabriqerr.CodeInvalidInput {
		t.Fatalf("fake backend must classify unknown entity as invalid_input, got %T %v", err, err)
	}
}
