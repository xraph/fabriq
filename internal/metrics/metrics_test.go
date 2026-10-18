package metrics

import (
	"strings"
	"testing"
	"time"

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
	m.ProjectionLag.WithLabelValues("graph").Set(3)
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

func TestScrapeTimeSeriesReadTheirSources(t *testing.T) {
	reg := prometheus.NewRegistry()
	depth := 7
	if _, err := New(reg,
		WithConflationDepth(func() int { return depth }),
		WithCatalogReads(func() (int64, int64, int64, bool) { return 10, 4, 1, true }),
	); err != nil {
		t.Fatal(err)
	}
	want := `
# HELP fabriq_catalog_read_failover_total Routing catalog read failover events (primary unreachable).
# TYPE fabriq_catalog_read_failover_total counter
fabriq_catalog_read_failover_total 1
# HELP fabriq_catalog_read_primary_total Routing catalog reads served by the primary.
# TYPE fabriq_catalog_read_primary_total counter
fabriq_catalog_read_primary_total 10
# HELP fabriq_catalog_read_replica_total Routing catalog reads served by a replica (failover).
# TYPE fabriq_catalog_read_replica_total counter
fabriq_catalog_read_replica_total 4
# HELP fabriq_conflation_depth Deltas buffered in this node's subscription-hub conflation windows.
# TYPE fabriq_conflation_depth gauge
fabriq_conflation_depth 7
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"fabriq_catalog_read_primary_total", "fabriq_catalog_read_replica_total",
		"fabriq_catalog_read_failover_total", "fabriq_conflation_depth"); err != nil {
		t.Fatal(err)
	}
	depth = 2
	if err := testutil.GatherAndCompare(reg, strings.NewReader(strings.Replace(want, "fabriq_conflation_depth 7", "fabriq_conflation_depth 2", 1)),
		"fabriq_conflation_depth"); err != nil {
		t.Fatal(err)
	}
}

func TestScrapeTimeSeriesWithoutSourcesReadZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	if _, err := New(reg, WithCatalogReads(func() (int64, int64, int64, bool) { return 9, 9, 9, false })); err != nil {
		t.Fatal(err)
	}
	if n, err := testutil.GatherAndCount(reg, "fabriq_conflation_depth", "fabriq_catalog_read_primary_total"); err != nil || n != 2 {
		t.Fatalf("series = %d, %v", n, err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		switch mf.GetName() {
		case "fabriq_conflation_depth":
			if v := mf.GetMetric()[0].GetGauge().GetValue(); v != 0 {
				t.Fatalf("conflation depth = %v", v)
			}
		case "fabriq_catalog_read_primary_total":
			if v := mf.GetMetric()[0].GetCounter().GetValue(); v != 0 {
				t.Fatalf("ok=false must read as zero, got %v", v)
			}
		}
	}
}

func TestDurationInstrumentsLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	if err != nil {
		t.Fatal(err)
	}
	m.ObserveCommand("create", "ok", 3*time.Millisecond)
	m.ObserveApply("graph", "error", time.Millisecond)
	m.ObserveRelayBatch("ok", time.Millisecond)
	m.ObserveModelCall("embed", "ok", 200*time.Millisecond)
	m.WorkerRestarts.WithLabelValues("relay").Inc()

	labels := map[string][]string{}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				labels[mf.GetName()] = append(labels[mf.GetName()], lp.GetName()+"="+lp.GetValue())
			}
		}
	}
	for name, want := range map[string]string{
		"fabriq_command_duration_seconds":          "op=create,result=ok",
		"fabriq_projection_apply_duration_seconds": "projection=graph,result=error",
		"fabriq_relay_batch_duration_seconds":      "result=ok",
		"fabriq_model_call_duration_seconds":       "kind=embed,result=ok",
		"fabriq_worker_restarts_total":             "runner=relay",
	} {
		if got := strings.Join(labels[name], ","); got != want {
			t.Fatalf("%s labels = %q, want %q", name, got, want)
		}
	}
}
