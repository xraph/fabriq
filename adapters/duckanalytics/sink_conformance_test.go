//go:build duckdb

package duckanalytics_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/duckanalytics"
)

func TestDuckAnalytics_OpenEnsuresSchema(t *testing.T) {
	ctx := context.Background()
	s, err := duckanalytics.Open(ctx, "duckdb://:memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
}
