//go:build duckdb

package duckanalytics_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/duckanalytics"
	"github.com/xraph/fabriq/core/analytics"
)

func TestDuckAnalytics_OpenEnsuresSchema(t *testing.T) {
	ctx := context.Background()
	s, err := duckanalytics.Open(ctx, "duckdb://:memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
}

func TestDuckAnalytics_Conformance(t *testing.T) {
	analytics.RunSinkConformance(t, func() analytics.Sink {
		s, err := duckanalytics.Open(context.Background(), "duckdb://:memory:")
		if err != nil {
			t.Fatal(err)
		}
		return s // a fresh in-memory DB per sub-test — no truncate/no-close wrapper needed
	})
}
