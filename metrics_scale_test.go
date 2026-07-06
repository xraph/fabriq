package fabriq

import (
	"testing"

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
