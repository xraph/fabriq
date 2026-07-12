package insights_test

import (
	"testing"

	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestFakeAnalytics_Conformance(t *testing.T) {
	insights.RunConformance(t, func() query.AnalyticsQuerier {
		return fabriqtest.NewFakeAnalytics()
	})
}
