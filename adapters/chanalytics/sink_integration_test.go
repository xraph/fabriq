//go:build integration

package chanalytics_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/chanalytics"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestChAnalytics_OpenEnsuresSchema(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartClickHouse(t)
	s, err := chanalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	// Re-open must be a no-op (IF NOT EXISTS).
	s2, err := chanalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	s2.Close()
}
