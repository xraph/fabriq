package analytics_test

import (
	"testing"

	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestFakeSink_Conformance(t *testing.T) {
	analytics.RunSinkConformance(t, func() analytics.Sink {
		return fabriqtest.NewFakeAnalyticsSink()
	})
}
