package command_test

import (
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// TestExec_UnknownEntityIsStructuredInvalidInput verifies that prepare (via
// the public Exec entry point) rejects an unregistered entity with a
// structured fabriqerr.Error carrying CodeInvalidInput and the entity name,
// so the admin write path can map it to HTTP 400 without string-matching.
func TestExec_UnknownEntityIsStructuredInvalidInput(t *testing.T) {
	x := newExecutor(t, newFakeStore())
	_, err := x.Exec(acmeCtx(t), command.Command{Entity: "products", Op: command.OpCreate, Payload: &cmdSite{}})
	if err == nil {
		t.Fatal("want error for unknown entity")
	}
	var fe *fabriqerr.Error
	if !errors.As(err, &fe) || fe.Code != fabriqerr.CodeInvalidInput {
		t.Fatalf("want invalid_input structured error, got %T %v", err, err)
	}
	if fe.Entity != "products" {
		t.Fatalf("entity not carried: %+v", fe)
	}
}
