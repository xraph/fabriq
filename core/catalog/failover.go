package catalog

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// Failover is a high-availability catalog.Catalog for catalog mode's routing
// read path (spec 2026-07-03 HA, H1-H3): it reads the PRIMARY first and falls
// through to read-only REPLICAS only when the primary is unreachable. Writes
// and any read that the primary answers definitively never touch a replica, so
// steady-state staleness is zero.
//
// It is injected into shard.CatalogDirectory in place of the bare primary. Every
// other catalog consumer (provisioning, adminapi, the sweeper scan, the
// reconciler elector) keeps the concrete primary store — those want the
// authoritative write node, not a possibly-lagged replica.
type Failover struct {
	primary  Catalog
	replicas []Catalog

	primaryReads  atomic.Int64
	replicaReads  atomic.Int64
	failoverReads atomic.Int64
}

var _ Catalog = (*Failover)(nil)

// NewFailover wraps a primary with zero or more read replicas. With no
// replicas it is a transparent pass-through to the primary.
func NewFailover(primary Catalog, replicas ...Catalog) *Failover {
	return &Failover{primary: primary, replicas: replicas}
}

// primaryDefinitive reports whether the primary gave a business answer (it is
// reachable) rather than a transport failure. A dead control DB surfaces as
// CodeInternal (renamed/absent DB, dial timeout) or a connection-class
// CodeUnavailable — never CodeNotFound, which is an authoritative "no such
// tenant". So nil or NotFound == the primary is up.
func primaryDefinitive(err error) bool {
	return err == nil || fabriqerr.CodeOf(err) == fabriqerr.CodeNotFound
}

// Get reads the primary first; on a transport failure it walks the replicas.
func (f *Failover) Get(ctx context.Context, tenantID string) (Entry, error) {
	e, err := f.primary.Get(ctx, tenantID)
	if primaryDefinitive(err) {
		f.primaryReads.Add(1)
		return e, err
	}
	primaryErr := err
	for _, r := range f.replicas {
		re, rerr := r.Get(ctx, tenantID)
		if rerr == nil {
			f.replicaReads.Add(1)
			f.failoverReads.Add(1)
			// Tag the provenance so the directory does not cache any error it
			// derives from this (possibly-lagged) entry — a version-gate or
			// not-active CodeUnavailable. The positive route itself is still
			// cached: routing continuity is the point of the fallback.
			re.FromReplica = true
			return re, nil
		}
		if fabriqerr.CodeOf(rerr) == fabriqerr.CodeNotFound {
			// A replica may be lagged: a "not found" here is NOT authoritative.
			// Mark it degraded so the directory never negative-caches it (H3).
			f.replicaReads.Add(1)
			f.failoverReads.Add(1)
			return Entry{}, degraded(tenantID)
		}
		// This replica is also unreachable — try the next one.
	}
	// Nothing reachable: return the primary's original (non-cacheable) error.
	return Entry{}, primaryErr
}

// List falls through to replicas on any primary error. Unlike Get, List has no
// definitive business answer to distinguish (no NotFound), so every primary
// error is a transport failure — replica-fallback needs no primaryDefinitive
// gate here. Only Get is wired to the routing directory in v1; List fallback is
// kept for interface coherence.
func (f *Failover) List(ctx context.Context, cursor Cursor, limit int) ([]Entry, Cursor, error) {
	out, next, err := f.primary.List(ctx, cursor, limit)
	if err == nil {
		return out, next, nil
	}
	primaryErr := err
	for _, r := range f.replicas {
		if ro, rn, rerr := r.List(ctx, cursor, limit); rerr == nil {
			return ro, rn, nil
		}
	}
	return nil, "", primaryErr
}

// Put is primary-only: fabriq never writes to a replica (H4, no split-brain).
func (f *Failover) Put(ctx context.Context, e Entry) (Entry, error) {
	return f.primary.Put(ctx, e)
}

// ReadStats returns cumulative routing-read counters (primary-served,
// replica-served, and failover events). Used by the observability poller.
func (f *Failover) ReadStats() (primary, replica, failover int64) {
	return f.primaryReads.Load(), f.replicaReads.Load(), f.failoverReads.Load()
}

// DegradedMetaKey / DegradedMetaValue are the fabriqerr Meta.Detail entry that
// flags a routing answer as served from a replica while the primary was
// unreachable. Failover stamps it on a replica NotFound, and the directory
// stamps it on any error it derives from a replica-sourced entry
// (MarkDegradedDetail); IsDegraded recognises it. The directory's cache
// predicate excludes degraded answers so a lagged replica can never pin a
// tenant for a TTL.
const (
	DegradedMetaKey   = "catalog"
	DegradedMetaValue = "degraded"
)

// MarkDegradedDetail stamps the degraded marker onto a Meta.Detail map,
// allocating one if nil, and returns it. Callers building a routing error from
// a replica-sourced entry use it so the directory will not cache the answer.
func MarkDegradedDetail(detail map[string]string) map[string]string {
	if detail == nil {
		detail = map[string]string{}
	}
	detail[DegradedMetaKey] = DegradedMetaValue
	return detail
}

func degraded(tenantID string) error {
	return fabriqerr.New(fabriqerr.CodeNotFound,
		"tenant not found on a catalog replica (primary unreachable; may be stale).",
		fabriqerr.WithEntity("tenant", tenantID),
		fabriqerr.WithMeta(fabriqerr.Meta{Detail: MarkDegradedDetail(nil)}))
}

// IsDegraded reports whether err is a replica-sourced answer served while the
// primary was unreachable — the directory must not negative-cache it.
func IsDegraded(err error) bool {
	var e *fabriqerr.Error
	if errors.As(err, &e) {
		return e.Meta.Detail[DegradedMetaKey] == DegradedMetaValue
	}
	return false
}
