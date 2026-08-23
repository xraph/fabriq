package insights_test

import (
	"testing"

	"github.com/xraph/fabriq/core/fabriqtest"
	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

func TestFakeAnalytics_Conformance(t *testing.T) {
	insights.RunConformance(t, func(reg *registry.Registry) query.AnalyticsQuerier {
		return fabriqtest.NewFakeAnalytics(reg)
	})
}
