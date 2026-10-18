package fabriq

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/xraph/fabriq/adapters/shard"
	"github.com/xraph/fabriq/internal/metrics"
)

func TestRecordScale_SetsCapAndCountsDirection(t *testing.T) {
	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	s := &Stores{}
	s.AttachMetrics(m)

	s.recordScale(shard.ScaleEvent{OldCap: 8, NewCap: 12, Direction: shard.DirGrow})
	s.recordScale(shard.ScaleEvent{OldCap: 12, NewCap: 11, Direction: shard.DirShrink})

	if v := testutil.ToFloat64(m.PoolCap); v != 11 {
		t.Fatalf("PoolCap=%v want 11 (last event)", v)
	}
	if v := testutil.ToFloat64(m.PoolScaleEvents.WithLabelValues("grow")); v != 1 {
		t.Fatalf("grow=%v want 1", v)
	}
	if v := testutil.ToFloat64(m.PoolScaleEvents.WithLabelValues("shrink")); v != 1 {
		t.Fatalf("shrink=%v want 1", v)
	}
}

// Catalog mode builds its relays inside Open, before the worker attaches
// metrics; their callbacks read the metrics when they fire.
func TestSweeperRelayCallbacksRecordOnceMetricsAttach(t *testing.T) {
	s := &Stores{}
	if got := len(s.relayMetricsOptions()); got != 2 {
		t.Fatalf("relay options = %d, want publish + batch", got)
	}
	s.recordRelayBatch("ok", time.Millisecond) // nothing attached yet: a no-op
	s.recordRelayPublished(3)

	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	s.AttachMetrics(m)
	s.recordRelayBatch("error", time.Millisecond)
	s.recordRelayPublished(5)

	if v := testutil.ToFloat64(m.RelayPublished); v != 5 {
		t.Fatalf("RelayPublished = %v, want 5 (only after attach)", v)
	}
	if n := testutil.CollectAndCount(m.RelayBatchDuration); n != 1 {
		t.Fatalf("relay batch series = %d, want 1", n)
	}
}
