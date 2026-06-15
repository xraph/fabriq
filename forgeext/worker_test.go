package forgeext_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/forgeext"
)

// TestRun_NoopWhenWorkerDisabled verifies that Run returns nil immediately
// without touching stores when RunWorker is false. Start is intentionally
// never called, so stores are nil; the RunWorker guard MUST fire first.
func TestRun_NoopWhenWorkerDisabled(t *testing.T) {
	reg := registry.New()
	ext := forgeext.New(reg,
		forgeext.WithConfig(fabriq.Config{
			Postgres: fabriq.PostgresConfig{DSN: "postgres://localhost/doesnotexist"},
		}),
		forgeext.WithWorker(false),
	)

	// Run must return nil without using stores (which are nil: Start never called).
	if err := ext.Run(context.Background()); err != nil {
		t.Fatalf("Run with RunWorker=false should be a no-op, got error: %v", err)
	}
}
