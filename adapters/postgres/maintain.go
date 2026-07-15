package postgres

import (
	"context"
	"hash/fnv"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/pathctx"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/sweep"
)

// Advisory lock keys per singleton worker role, per database. Stable across
// versions; never reuse a key for a different role. In the static plane the
// forgeext worker holds these for the process lifetime (leader election);
// in catalog mode the sweeper try-claims them per pass — the SAME keys, so
// the two modes can never both work one database.
const (
	// LockKeyRelay guards the outbox relay.
	LockKeyRelay = int64(1001)
	// LockKeyReconciler guards the projection drift reconciler.
	LockKeyReconciler = int64(1002)
	// LockKeyDocumentPlane guards the document materializer + compactor.
	LockKeyDocumentPlane = int64(1003)
	// LockKeyBlobGC guards the blob CAS garbage collector.
	LockKeyBlobGC = int64(1004)
	// LockKeyRollup guards the rollup:insights materialized-rollup maintainer
	// (phase 2b).
	LockKeyRollup = int64(1005)
)

// schemaLockKey derives a per-SCHEMA advisory lock key for schema-per-tenant
// consolidation mode, where many tenants share one database and the static
// per-database keys above would serialize every tenant's maintenance behind
// one lock. The role occupies the HIGH 32 bits and an FNV-1a-32 of the schema
// the LOW 32 bits: role is recoverable (key>>32), cross-role collisions are
// impossible (distinct high bits), and the static keys (high bits zero) can
// never collide with a schema key. A within-role schema-hash collision only
// costs false serialization of two tenants — never a missed lock.
func schemaLockKey(base int64, schema string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(schema))
	return (base << 32) | int64(h.Sum32())
}

// LockKeyRelayFor is the schema-scoped relay lock key.
func LockKeyRelayFor(schema string) int64 { return schemaLockKey(LockKeyRelay, schema) }

// LockKeyReconcilerFor is the schema-scoped reconciler lock key.
func LockKeyReconcilerFor(schema string) int64 { return schemaLockKey(LockKeyReconciler, schema) }

// LockKeyDocumentPlaneFor is the schema-scoped document-plane lock key.
func LockKeyDocumentPlaneFor(schema string) int64 {
	return schemaLockKey(LockKeyDocumentPlane, schema)
}

// LockKeyBlobGCFor is the schema-scoped blob-GC lock key.
func LockKeyBlobGCFor(schema string) int64 { return schemaLockKey(LockKeyBlobGC, schema) }

// Maintenance is one tenant database's single-pass worker surface (spec
// 2026-07-03 D5): the claim-guarded form of the loops the static worker
// runs boot-time. Each Sweep try-claims the database's own advisory locks,
// so any number of sweeper replicas cooperate — losers skip cleanly.
type Maintenance struct {
	a     *Adapter
	relay *Relay // nil without a publisher: events stay in the outbox
	docs  *DocStore
}

// NewMaintenance builds the maintenance surface for one tenant database.
// docs MUST be the SAME DocStore the serving plane uses (archive-enabled
// when history offload is configured) — minting a fresh a.Documents() here
// would silently skip history sealing on compaction. pub may be nil (no
// event transport): the relay phase is skipped and outbox rows accumulate.
func NewMaintenance(a *Adapter, reg *registry.Registry, pub event.Publisher, docs *DocStore) *Maintenance {
	m := &Maintenance{a: a, docs: docs}
	if pub != nil {
		m.relay = NewRelay(a, reg, pub)
	}
	return m
}

// lockKeys returns the (relay, document-plane) advisory-lock keys for this
// pass. In schema-per-tenant consolidation mode the sweeper puts the tenant
// schema on ctx and the keys fold it in, so tenants sharing a database claim
// distinct locks (and can maintain concurrently); otherwise the static
// per-database keys apply (one database == one tenant).
func (m *Maintenance) lockKeys(ctx context.Context) (relay, doc int64) {
	if schema := pathctx.SchemaOrEmpty(ctx); schema != "" {
		return LockKeyRelayFor(schema), LockKeyDocumentPlaneFor(schema)
	}
	return LockKeyRelay, LockKeyDocumentPlane
}

// Sweep implements sweep.Maintainer: one maintenance pass over this
// database — relay the outbox, materialize quiet documents, and (when
// compact is set) compact due document logs. Lock losers report
// Claimed=false with whatever the won phases did. Electors are minted per
// pass so their advisory-lock keys reflect the ctx schema (schema mode).
func (m *Maintenance) Sweep(ctx context.Context, compact bool) (sweep.Result, error) {
	var res sweep.Result
	relayKey, docKey := m.lockKeys(ctx)

	if m.relay != nil {
		led, err := NewElector(m.a, relayKey).TryLead(ctx, func(c context.Context) error {
			n, derr := m.relay.DrainAll(c)
			res.Relayed = n
			return derr
		})
		if err != nil {
			return res, err
		}
		res.Claimed = res.Claimed || led
	}

	led, err := NewElector(m.a, docKey).TryLead(ctx, func(c context.Context) error {
		n, merr := m.docs.MaterializeQuiet(c, nil)
		if merr != nil {
			return merr
		}
		res.Materialized = n
		if compact {
			cn, cerr := m.docs.CompactDue(c)
			if cerr != nil {
				return cerr
			}
			res.Compacted = cn
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	res.Claimed = res.Claimed || led
	return res, nil
}

var _ sweep.Maintainer = (*Maintenance)(nil)
