//go:build integration

package pganalytics_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/pganalytics"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestPgAnalytics_OpenEnsuresSchema(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	s, err := pganalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	// Re-open must be a no-op (IF NOT EXISTS).
	s2, err := pganalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	s2.Close()
}
