package analytics

import (
	"context"
	"fmt"
	"sync"

	"github.com/xraph/fabriq/core/event"
)

// SnapshotFunc streams a tenant's current-state envelopes (satisfied by
// postgres.Adapter.SnapshotEntities and the catalog DynamicSet-routed
// snapshot). Backfill drives the SAME applier as live consume, so the two
// agree; the sink's version gate makes a re-run a no-op and lets live traffic
// run concurrently.
type SnapshotFunc func(ctx context.Context, tenantID string, fn func(event.Envelope) error) error

// Backfiller replays a tenant snapshot into the analytics sink.
type Backfiller struct {
	Snapshot SnapshotFunc
	Applier  *Applier
	Sink     Sink
	Batch    int // default 128
}

// Tenant backfills one tenant, returning the count of analyticized rows.
func (b *Backfiller) Tenant(ctx context.Context, tenantID string) (int, error) {
	batch := b.Batch
	if batch <= 0 {
		batch = 128
	}
	var facts []Fact
	var events []Event
	var wms []Watermark
	rows := 0

	flush := func() error {
		if len(facts) == 0 {
			return nil
		}
		if err := b.Sink.UpsertFacts(ctx, facts); err != nil {
			return err
		}
		if err := b.Sink.AppendEvents(ctx, events); err != nil {
			return err
		}
		if err := b.Sink.SetWatermark(ctx, wms); err != nil {
			return err
		}
		facts, events, wms = facts[:0], events[:0], wms[:0]
		return nil
	}

	err := b.Snapshot(ctx, tenantID, func(env event.Envelope) error {
		fact, ev, ok, err := b.Applier.Apply(env)
		if err != nil || !ok {
			return err // skip unmarked/malformed (err is nil for unmarked)
		}
		rows++
		facts = append(facts, fact)
		events = append(events, ev)
		wms = append(wms, Watermark{TenantID: env.TenantID, Aggregate: env.Aggregate, AggID: env.AggID, Version: env.Version})
		if len(facts) >= batch {
			return flush()
		}
		return nil
	})
	if err != nil {
		return rows, err
	}
	return rows, flush()
}

// AllTenants backfills each tenant with bounded concurrency. One tenant's
// failure is recorded in the returned error but does not abort the others
// (fleet-wide operation). Concurrency <= 0 defaults to 4.
func (b *Backfiller) AllTenants(ctx context.Context, tenants []string, concurrency int) (map[string]int, error) {
	if concurrency <= 0 {
		concurrency = 4
	}
	sem := make(chan struct{}, concurrency)
	var mu sync.Mutex
	counts := make(map[string]int, len(tenants))
	var firstErr error
	var wg sync.WaitGroup

	for _, tn := range tenants {
		wg.Add(1)
		sem <- struct{}{}
		go func(tn string) {
			defer wg.Done()
			defer func() { <-sem }()
			n, err := b.Tenant(ctx, tn)
			mu.Lock()
			counts[tn] = n
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("fabriq: analytics backfill tenant %s: %w", tn, err)
			}
			mu.Unlock()
		}(tn)
	}
	wg.Wait()
	return counts, firstErr
}
