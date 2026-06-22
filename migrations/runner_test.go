package migrations

import (
	"context"
	"testing"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
)

// TestNewOrchestratorFromGrove builds an orchestrator on a borrowed grove.DB
// without dialing (pgdriver.New() is unopened) and proves the close func is a
// no-op that leaves the borrowed handle intact.
func TestNewOrchestratorFromGrove(t *testing.T) {
	if _, _, err := NewOrchestratorFromGrove(nil); err == nil {
		t.Fatal("nil grove must error")
	}

	gdb, err := grove.Open(pgdriver.New())
	if err != nil {
		t.Fatalf("grove.Open: %v", err)
	}
	orch, closeFn, err := NewOrchestratorFromGrove(gdb)
	if err != nil {
		t.Fatalf("NewOrchestratorFromGrove: %v", err)
	}
	if orch == nil {
		t.Fatal("nil orchestrator")
	}
	if err := closeFn(); err != nil {
		t.Fatalf("close on borrowed grove should be a no-op: %v", err)
	}
	// The borrowed grove must still be open after the orchestrator's close.
	if err := gdb.Close(); err != nil {
		t.Fatalf("borrowed grove was torn down by orchestrator close: %v", err)
	}
}

// TestOpenOrchestratorWith verifies DSN-preference, grove fallback, and the
// no-target error path. The DSN-dial branch needs Postgres, so it is exercised
// by the integration suite; here we cover the routing logic.
func TestOpenOrchestratorWith(t *testing.T) {
	if _, _, err := OpenOrchestratorWith(context.Background(), "", nil); err == nil {
		t.Fatal("no dsn and no grove must error")
	}

	gdb, err := grove.Open(pgdriver.New())
	if err != nil {
		t.Fatalf("grove.Open: %v", err)
	}
	orch, closeFn, err := OpenOrchestratorWith(context.Background(), "", gdb)
	if err != nil {
		t.Fatalf("OpenOrchestratorWith grove fallback: %v", err)
	}
	defer func() { _ = closeFn() }()
	if orch == nil {
		t.Fatal("nil orchestrator from grove fallback")
	}
}
