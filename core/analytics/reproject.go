package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/xraph/fabriq/core/registry"
)

// Reprojector retroactively applies each analytics-marked entity's CURRENT
// redaction allow-list to already-stored facts and events. It is the privacy
// correction the version gate otherwise blocks: tightening an entity's
// `Include` (or dropping a field) does not re-redact existing rows on its own,
// because a same-version backfill is a no-op. Reprojection rewrites the stored
// payloads in place through the current spec.
//
// It can only NARROW: it re-projects the already-stored (possibly wider)
// payload, so it removes fields that should no longer be co-located. Widening
// (bringing a previously-stripped field back) is not possible from stored data
// — that needs a forced backfill from the source database, out of scope here.
type Reprojector struct {
	Reg  *registry.Registry
	Sink Sink
}

// Tenant re-projects every analytics-marked aggregate's stored payloads for one
// tenant through its current spec, returning the number of rows rewritten.
func (r *Reprojector) Tenant(ctx context.Context, tenantID string) (int64, error) {
	var total int64
	for _, ent := range r.Reg.All() {
		spec := ent.Spec.Analytics
		if spec == nil {
			continue
		}
		aggregate := ent.Spec.Name
		transform := func(p json.RawMessage) (json.RawMessage, error) { return Redact(p, spec) }
		n, err := r.Sink.ReprojectTenant(ctx, tenantID, aggregate, transform)
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject %s/%s: %w", tenantID, aggregate, err)
		}
		total += n
	}
	return total, nil
}

// AllTenants reprojects each tenant with bounded concurrency. One tenant's
// failure is recorded (first error returned) but does not abort the others.
// Concurrency <= 0 defaults to 4.
func (r *Reprojector) AllTenants(ctx context.Context, tenants []string, concurrency int) (map[string]int64, error) {
	if concurrency <= 0 {
		concurrency = 4
	}
	sem := make(chan struct{}, concurrency)
	var mu sync.Mutex
	counts := make(map[string]int64, len(tenants))
	var firstErr error
	var wg sync.WaitGroup

	for _, tn := range tenants {
		wg.Add(1)
		sem <- struct{}{}
		go func(tn string) {
			defer wg.Done()
			defer func() { <-sem }()
			n, err := r.Tenant(ctx, tn)
			mu.Lock()
			counts[tn] = n
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("fabriq: analytics reproject tenant %s: %w", tn, err)
			}
			mu.Unlock()
		}(tn)
	}
	wg.Wait()
	return counts, firstErr
}
