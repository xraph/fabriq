package main

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// telemetrySeries is the physical telemetry table the demo writes readings into.
// It is the example-domain tag_readings table (migration 0003), addressed by the
// TSQuerier by its table name. On the plain-Postgres demo stack it stays a plain
// table (the timescaledb hypertable conversion in migration 0005 skips quietly),
// so BulkWrite/Range work without the extension.
const telemetrySeries = "tag_readings"

// telemetryWindow is how far back the seeded readings stretch from now, and
// telemetryStep is the spacing between samples. 24h at 5-minute spacing is 288
// points per key — enough to render a meaningful line and exercise in-memory
// bucketing without a heavy write.
const (
	telemetryWindow = 24 * time.Hour
	telemetryStep   = 5 * time.Minute
)

// telemetryTag describes one seeded series key and the deterministic wave its
// readings follow, so the Telemetry view has visibly distinct signals.
type telemetryTag struct {
	// key is the series key (tag id) written into tag_readings.tag_id.
	key string
	// base/amplitude/periodH shape a sine wave: base + amplitude*sin(2π t/periodH).
	base      float64
	amplitude float64
	periodH   float64
	// jitter is a small deterministic per-sample wobble so the line is not a
	// perfectly clean sine (looks like real telemetry).
	jitter float64
}

// telemetryTags is the fixed set of demo signals. Two tenants share the same
// keys but get phase-shifted waves (see seedTelemetry) so tenant isolation is
// visible when switching the tenant in the console.
var telemetryTags = []telemetryTag{
	{key: "cpu.load", base: 45, amplitude: 30, periodH: 6, jitter: 4},
	{key: "mem.used.pct", base: 62, amplitude: 15, periodH: 12, jitter: 2},
	{key: "requests.rate", base: 800, amplitude: 550, periodH: 8, jitter: 40},
	{key: "latency.p95.ms", base: 120, amplitude: 70, periodH: 4, jitter: 10},
}

// seedTelemetry idempotently writes a day of deterministic readings for each
// demo signal into the telemetry plane via the DIRECT bulk-write path
// (Timeseries().BulkWrite) — the same event-bypass path real bulk telemetry
// uses, invoked inline so the demo needs no ingest worker. It returns the number
// of points written (0 when the tenant is already seeded).
//
// Idempotency: it probes the most recent hour of the first tag; if any readings
// already exist for this tenant it skips entirely, so re-running on every startup
// does not pile up duplicate points (tag_readings has no natural key to upsert
// against, so a presence check is the guard).
func seedTelemetry(ctx context.Context, f *fabriq.Fabriq, tid string) (int, error) {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return 0, fmt.Errorf("admin-demo: telemetry tenant %q: %w", tid, err)
	}

	now := time.Now().UTC()

	// Presence check: any point for the first tag in the last window means this
	// tenant is already seeded.
	var existing []query.Point
	probe := query.RangeQuery{
		Series: telemetrySeries,
		Key:    telemetryTags[0].key,
		From:   now.Add(-telemetryWindow),
		To:     now.Add(time.Minute),
	}
	if perr := f.Timeseries().Range(tctx, probe, &existing); perr != nil {
		return 0, fmt.Errorf("admin-demo: telemetry probe for %q: %w", tid, perr)
	}
	if len(existing) > 0 {
		return 0, nil // already seeded — safe to re-run on every startup
	}

	// Phase-shift each tenant so the same key reads differently per tenant
	// (deterministic, derived from the tenant id length — no randomness).
	phase := float64(len(tid)) * 0.7

	start := now.Add(-telemetryWindow)
	total := 0
	for _, tag := range telemetryTags {
		points := make([]query.Point, 0, int(telemetryWindow/telemetryStep)+1)
		i := 0
		for t := start; !t.After(now); t = t.Add(telemetryStep) {
			hours := t.Sub(start).Hours()
			// Deterministic sine + a small triangle-ish jitter from the sample index.
			wave := tag.base + tag.amplitude*math.Sin(2*math.Pi*(hours/tag.periodH)+phase)
			wobble := tag.jitter * math.Sin(float64(i)*1.3+phase)
			v := wave + wobble
			if v < 0 {
				v = 0
			}
			points = append(points, query.Point{
				Key:     tag.key,
				At:      t,
				Value:   round2(v),
				Quality: 192, // OPC "good" quality band — illustrative, non-zero
			})
			i++
		}
		if werr := f.Timeseries().BulkWrite(tctx, telemetrySeries, points); werr != nil {
			return total, fmt.Errorf("admin-demo: telemetry bulk write %s for %q: %w", tag.key, tid, werr)
		}
		total += len(points)
	}
	return total, nil
}

// round2 rounds to two decimal places so the seeded readings are tidy in the UI.
func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
