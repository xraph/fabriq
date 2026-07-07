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

func TestBlobGCInstruments(t *testing.T) {
	m, err := New(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// All five instruments are wired (no nil deref when emitting).
	m.BlobGCBytesFreed.Add(1024)
	m.BlobGCCollected.Add(3)
	m.BlobGCRefDriftCorrected.Add(2)
	m.BlobGCBroken.Set(1)
	m.BlobGCOrphans.Add(4)
}

func TestAnalyticsInstruments(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	m.AnalyticsAppliedTotal.Inc()
	m.AnalyticsFailuresTotal.Inc()
	m.AnalyticsLagSeconds.Set(12.5)
	m.AnalyticsTenantsBehind.Set(3)
	m.AnalyticsEventsPrunedTotal.Add(7)

	if got := testutil.ToFloat64(m.AnalyticsAppliedTotal); got != 1 {
		t.Fatalf("AnalyticsAppliedTotal = %v", got)
	}
	if got := testutil.ToFloat64(m.AnalyticsFailuresTotal); got != 1 {
		t.Fatalf("AnalyticsFailuresTotal = %v", got)
	}
	if got := testutil.ToFloat64(m.AnalyticsLagSeconds); got != 12.5 {
		t.Fatalf("AnalyticsLagSeconds = %v", got)
	}
	if got := testutil.ToFloat64(m.AnalyticsTenantsBehind); got != 3 {
		t.Fatalf("AnalyticsTenantsBehind = %v", got)
	}
	if got := testutil.ToFloat64(m.AnalyticsEventsPrunedTotal); got != 7 {
		t.Fatalf("AnalyticsEventsPrunedTotal = %v", got)
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
	for _, want := range []string{"fabriq_analytics_applied_total", "fabriq_analytics_failures_total", "fabriq_analytics_lag_seconds", "fabriq_analytics_tenants_behind", "fabriq_analytics_events_pruned_total"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("metric %q not registered (have %s)", want, joined)
		}
	}
}
