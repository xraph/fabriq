package forgeext

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/xraph/forge"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/internal/metrics"
)

const producerTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func newTestApp(t *testing.T) forge.App {
	t.Helper()
	return forge.New(forge.WithAppName("fabriq-telemetry-test"), forge.WithAppLogger(forge.NewNoopLogger()))
}

func scrapeRoute(t *testing.T, app forge.App, path string) string {
	t.Helper()
	code, body := getRoute(t, app, path)
	if code != http.StatusOK {
		t.Fatalf("GET %s = %d", path, code)
	}
	return body
}

func getRoute(t *testing.T, app forge.App, path string) (int, string) {
	t.Helper()
	rr := httptest.NewRecorder()
	app.Router().ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	body, _ := io.ReadAll(rr.Body)
	return rr.Code, string(body)
}

// fabriq's registry stays private: GET /metrics serves fabriq_ names with no
// forge_ prefix, and the series that used to be wrong are fixed (the catalog
// read counters are counters, lag has no tenant label, conflation depth is
// live).
func TestMetricsEndpointServesFabriqSeries(t *testing.T) {
	app := newTestApp(t)
	e := New(embedReg(t))
	if err := e.Register(app); err != nil {
		t.Fatal(err)
	}
	m := e.initMetrics()
	if m == nil {
		t.Fatal("no metrics")
	}
	if again := e.initMetrics(); again != m {
		t.Fatal("initMetrics must build the registry once")
	}
	// The route is not taken during registration: a host extension that
	// registers after fabriq still gets the chance first.
	if code, _ := getRoute(t, app, "/metrics"); code == http.StatusOK {
		t.Fatal("/metrics must not be mounted before Run")
	}
	// Run mounts it even without the worker, and only once.
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	m.ObserveCommand("create", "ok", 2*time.Millisecond)
	m.ProjectionLag.WithLabelValues("graph").Set(4)
	m.WorkerRestarts.WithLabelValues(runnerLabel("relay:shard-7")).Inc()

	body := scrapeRoute(t, app, "/metrics")
	for _, want := range []string{
		`fabriq_command_duration_seconds_count{op="create",result="ok"} 1`,
		`fabriq_projection_lag_events{projection="graph"} 4`,
		`fabriq_worker_restarts_total{runner="relay"} 1`,
		"# TYPE fabriq_catalog_read_primary_total counter",
		"# TYPE fabriq_conflation_depth gauge",
		"# TYPE fabriq_tenant_hook_trips_total counter",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("GET /metrics lacks %q", want)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "forge_") || strings.Contains(line, "shard-7") || strings.Contains(line, `tenant="`) {
			t.Fatalf("unexpected series on fabriq's registry: %q", line)
		}
	}
}

// Start hands fabriq.Open the app's metrics, so grove records
// db_client_operation_duration_seconds on forge's /_/metrics.
func TestOpenOptionsCarryTheAppMetrics(t *testing.T) {
	e := New(embedReg(t))
	if got := len(e.openOptions()); got != 2 {
		t.Fatalf("unregistered: %d options, want hooks + command observer", got)
	}
	if err := e.Register(newTestApp(t)); err != nil {
		t.Fatal(err)
	}
	if e.Metrics() == nil {
		t.Fatal("a registered extension must see the app's metrics")
	}
	if got := len(e.openOptions()); got != 3 {
		t.Fatalf("registered: %d options, want hooks + command observer + grove metrics", got)
	}
}

type cancelOnError struct {
	forge.Logger
	cancel func()
}

func (l cancelOnError) Error(string, ...forge.Field) { l.cancel() }

func TestSuperviseCountsRestartsWithoutShardIDs(t *testing.T) {
	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	log := cancelOnError{Logger: forge.NewNoopLogger(), cancel: cancel}
	supervise(ctx, log, m, "relay:shard-7", func(context.Context) error { return errors.New("redis down") })

	if got := testutil.ToFloat64(m.WorkerRestarts.WithLabelValues("relay")); got != 1 {
		t.Fatalf("restarts{runner=relay} = %v, want 1", got)
	}
	for in, want := range map[string]string{"relay": "relay", "relay:0": "relay", "proj:graph": "proj:graph", "sweeper:wake": "sweeper:wake"} {
		if got := runnerLabel(in); got != want {
			t.Fatalf("runnerLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEmbedHandlerSpans(t *testing.T) {
	rec := telemetrytest.Record(t)
	reg := embedReg(t)
	ix, err := agent.NewIndexer(fabriqtest.NewFabric(fabriqtest.NewWorld(reg)), reg, stubEmb{})
	if err != nil {
		t.Fatal(err)
	}
	boot, bootSpan := telemetry.Tracer().Start(context.Background(), "start fabriq")
	h := embedHandler(telemetry.Detach(boot), ix, nil)

	env := event.Envelope{TenantID: "acme", Aggregate: "ewdoc", AggID: "d1", Type: "ewdoc.created",
		Payload: json.RawMessage(`{"title":"alice@example.com"}`), Traceparent: producerTP}
	if err := h("s", env); err != nil {
		t.Fatal(err)
	}
	other := env
	other.Aggregate = "not-embeddable"
	if err := h("s", other); err != nil {
		t.Fatal(err)
	}
	bootSpan.End()

	spans := telemetrytest.Named(rec, "embed event")
	if len(spans) != 1 {
		t.Fatalf("got %d embed spans, want 1 (non-embeddable entities mint none)", len(spans))
	}
	s := spans[0]
	if s.SpanKind() != trace.SpanKindConsumer || s.Parent().IsValid() || len(s.Links()) != 1 {
		t.Fatalf("want a linked root consumer span: kind=%v parent=%v links=%d", s.SpanKind(), s.Parent(), len(s.Links()))
	}
	if calls := telemetrytest.Named(rec, "call embedder"); len(calls) != 1 || calls[0].Parent().SpanID() != s.SpanContext().SpanID() {
		t.Fatal("the model call must be a child of the embed span")
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
	telemetrytest.AssertNoValue(t, rec, "acme")
	telemetrytest.AssertNoValue(t, rec, "d1")
}

type baggageEmb struct{ stubEmb }

var errProvider = errors.New("provider rejected alice@example.com")

func (baggageEmb) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if baggage.FromContext(ctx).Len() != 0 {
		return nil, errors.New("baggage reached the provider")
	}
	if len(texts) == 1 && texts[0] == "fail" {
		return nil, errProvider
	}
	return stubEmb{}.Embed(ctx, texts)
}

func TestMeteredModelsRecordDurationAndResult(t *testing.T) {
	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if meteredEmbedder(nil, m) != nil || meteredSummarizer(nil, m) != nil {
		t.Fatal("a nil model must stay nil")
	}
	if got := meteredEmbedder(stubEmb{}, nil); got != (stubEmb{}) {
		t.Fatal("nil metrics must pass the embedder through")
	}
	emb := meteredEmbedder(baggageEmb{}, m)
	if _, err := emb.Embed(context.Background(), []string{"ok"}); err != nil {
		t.Fatal(err)
	}
	if _, err := emb.Embed(context.Background(), []string{"fail"}); !errors.Is(err, errProvider) {
		t.Fatalf("err = %v", err)
	}
	sum := meteredSummarizer(fakeSum{}, m)
	if _, err := sum.Summarize(context.Background(), agent.SummaryInput{Raw: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	for labels, want := range map[[2]string]uint64{{"embed", "ok"}: 1, {"embed", "error"}: 1, {"summarize", "ok"}: 1} {
		h := m.ModelCallDuration.WithLabelValues(labels[0], labels[1]).(prometheus.Histogram)
		if got := histogramCount(t, h); got != want {
			t.Fatalf("model calls %v = %d, want %d", labels, got, want)
		}
	}
}

func histogramCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	if err := reg.Register(h); err != nil {
		t.Fatal(err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	return mfs[0].GetMetric()[0].GetHistogram().GetSampleCount()
}

func TestDistillSweepIsOneRootSpan(t *testing.T) {
	rec := telemetrytest.Record(t)
	reg := distillReg(t)
	d, err := agent.NewDistiller(fabriqtest.NewFabric(fabriqtest.NewWorld(reg)), reg, stubEmb{}, fakeSum{}, nil, fabriqtest.NewFakeCAS(), agent.DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	sw := newDistillSweeper(d, time.Hour, time.Hour, nil)
	env := event.Envelope{TenantID: "acme", Aggregate: "note", AggID: "n1", Type: "note.created",
		Payload: json.RawMessage(`{"id":"n1","title":"alice@example.com","body":"vibration","site_id":"s1"}`)}
	sw.mu.Lock()
	sw.dirty["acme"] = map[ref]event.Envelope{{entity: "note", id: "n1"}: env}
	sw.mu.Unlock()
	sw.sweep("acme")

	spans := telemetrytest.Named(rec, "distill tenant")
	if len(spans) != 1 || spans[0].Parent().IsValid() {
		t.Fatalf("want one root distill span, got %d", len(spans))
	}
	if n, _ := telemetrytest.Attr(spans[0], "fabriq.distill.refs"); n != "1" {
		t.Fatalf("fabriq.distill.refs = %q", n)
	}
	if spans[0].Status().Code == codes.Error {
		t.Fatalf("status = %+v", spans[0].Status())
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
	telemetrytest.AssertNoValue(t, rec, "acme")
}

func TestReconcileTracedIsARootPerProjection(t *testing.T) {
	rec := telemetrytest.Record(t)
	reg := embedReg(t)
	r := &projection.Reconciler{
		Projection: "graph",
		Registry:   reg,
		Truth: func(context.Context, string, string) (map[string]int64, error) {
			return nil, errors.New("pg down for tenant acme")
		},
		Projected: func(context.Context, string, *registry.Entity) (map[string]int64, error) { return nil, nil },
	}
	boot, bootSpan := telemetry.Tracer().Start(context.Background(), "start fabriq")
	reconcileTraced(boot, r, "graph", "acme")
	bootSpan.End()

	s := telemetrytest.Named(rec, "reconcile projection")
	if len(s) != 1 || s[0].Parent().IsValid() {
		t.Fatalf("want one root reconcile span, got %d", len(s))
	}
	if p, _ := telemetrytest.Attr(s[0], "fabriq.projection"); p != "graph" {
		t.Fatalf("fabriq.projection = %q", p)
	}
	if s[0].Status().Description != "reconcile failed" {
		t.Fatalf("status = %+v", s[0].Status())
	}
	telemetrytest.AssertNoValue(t, rec, "acme")
}

// retentionSink records whether its calls ran under a span and how many
// events it pruned.
type retentionSink struct {
	*fabriqtest.FakeAnalyticsSink
	parents []trace.SpanContext
}

func (s *retentionSink) MaintainPartitions(ctx context.Context, _ time.Duration) (int, int, error) {
	s.parents = append(s.parents, trace.SpanContextFromContext(ctx))
	return 0, 0, nil
}

func (s *retentionSink) PruneEvents(ctx context.Context, _ time.Time) (int64, error) {
	s.parents = append(s.parents, trace.SpanContextFromContext(ctx))
	return 3, nil
}

func TestAnalyticsRetentionRunIsARootSpan(t *testing.T) {
	rec := telemetrytest.Record(t)
	sink := &retentionSink{FakeAnalyticsSink: fabriqtest.NewFakeAnalyticsSink()}
	boot, bootSpan := telemetry.Tracer().Start(context.Background(), "start fabriq")
	ctx, cancel := context.WithCancel(boot)
	cancel() // the startup run still happens; the loop then exits
	analyticsRetentionLoop(ctx, nil, sink, time.Hour)
	bootSpan.End()

	s := telemetrytest.Named(rec, "enforce analytics retention")
	if len(s) != 1 || s[0].Parent().IsValid() {
		t.Fatalf("want one root retention span, got %d", len(s))
	}
	if n, _ := telemetrytest.Attr(s[0], "fabriq.analytics.events_pruned"); n != "3" {
		t.Fatalf("fabriq.analytics.events_pruned = %q", n)
	}
	for _, p := range sink.parents {
		if p.SpanID() != s[0].SpanContext().SpanID() {
			t.Fatal("the sink calls must run under the retention span")
		}
	}
}

// pollGauges starts under forge's boot span; its reads must not see it.
func TestPollGaugesDropsTheInheritedSpan(t *testing.T) {
	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	type read struct {
		what  string
		valid bool
	}
	reads := make(chan read, 8)
	src := gaugeReads{
		backlog: func(ctx context.Context) (int64, bool) {
			reads <- read{"backlog", trace.SpanContextFromContext(ctx).IsValid()}
			return 7, true
		},
		groupLag: func(ctx context.Context, group string) (int64, error) {
			reads <- read{group, trace.SpanContextFromContext(ctx).IsValid()}
			return 3, nil
		},
	}
	boot, span := telemetry.Tracer().Start(context.Background(), "start fabriq")
	defer span.End()
	ctx, cancel := context.WithCancel(boot)
	done := make(chan struct{})
	go func() {
		defer close(done)
		pollGauges(ctx, src, m, time.Millisecond)
	}()
	for _, want := range []string{"backlog", "proj:graph", "proj:search"} {
		r := <-reads
		if r.what != want {
			t.Fatalf("read %q, want %q", r.what, want)
		}
		if r.valid {
			t.Fatalf("the %s read ran under the boot span", r.what)
		}
	}
	cancel()
	<-done
	if got := testutil.ToFloat64(m.OutboxBacklog); got != 7 {
		t.Fatalf("fabriq_outbox_backlog = %v, want 7", got)
	}
	if got := testutil.ToFloat64(m.ProjectionLag.WithLabelValues("search")); got != 3 {
		t.Fatalf("fabriq_projection_lag_events{search} = %v, want 3", got)
	}
}
