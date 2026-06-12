package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics_RegisterAndObserve(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	if err != nil {
		t.Fatal(err)
	}

	m.OutboxBacklog.Set(42)
	m.TenantHookTrips.Inc()
	m.ConflationDepth.Set(7)
	m.ProjectionLag.WithLabelValues("graph", "acme").Set(3)
	m.RelayPublished.Add(128)

	if got := testutil.ToFloat64(m.OutboxBacklog); got != 42 {
		t.Fatalf("OutboxBacklog = %v", got)
	}
	if got := testutil.ToFloat64(m.TenantHookTrips); got != 1 {
		t.Fatalf("TenantHookTrips = %v", got)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	joined := strings.Join(names, ",")
	for _, want := range []string{
		"fabriq_outbox_backlog",
		"fabriq_tenant_hook_trips_total",
		"fabriq_conflation_depth",
		"fabriq_projection_lag_events",
		"fabriq_relay_published_total",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("metric %q not registered (have %s)", want, joined)
		}
	}
}

func TestMetrics_DoubleRegisterFails(t *testing.T) {
	reg := prometheus.NewRegistry()
	if _, err := New(reg); err != nil {
		t.Fatal(err)
	}
	if _, err := New(reg); err == nil {
		t.Fatal("double registration must fail loudly")
	}
}
