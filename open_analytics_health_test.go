package fabriq

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

type unhealthyAnalytics struct{ *fabriqtest.FakeAnalyticsSink }

func (*unhealthyAnalytics) LagByTenant(context.Context) (map[string]float64, error) {
	return nil, errors.New("analytics unavailable")
}

func TestAnalyticsHealthDetectsOutage(t *testing.T) {
	stores := &Stores{}
	if err := stores.PingAnalytics(context.Background()); err != nil {
		t.Fatal(err)
	}
	stores.Analytics = &unhealthyAnalytics{fabriqtest.NewFakeAnalyticsSink()}
	if err := stores.PingAnalytics(context.Background()); err == nil {
		t.Fatal("configured sink outage hidden")
	}
}
