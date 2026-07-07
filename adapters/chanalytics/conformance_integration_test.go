//go:build integration

package chanalytics_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/chanalytics"
	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/fabriqtest"
)

// noCloseSink shares one *chanalytics.Sink across the suite; RunSinkConformance's
// per-sub-test Close must not tear down the shared connection. Each newSink()
// truncates for isolation (mirrors the pganalytics conformance harness).
type noCloseSink struct{ *chanalytics.Sink }

func (noCloseSink) Close() error { return nil }

func TestChAnalytics_Conformance(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartClickHouse(t)
	s, err := chanalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	analytics.RunSinkConformance(t, func() analytics.Sink {
		if err := chanalytics.TruncateForTest(ctx, s); err != nil {
			t.Fatal(err)
		}
		return noCloseSink{s}
	})
}
