package postgres

import (
	"context"

	"github.com/xraph/fabriq/core/event"
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
)

// Maintenance is one tenant database's single-pass worker surface (spec
// 2026-07-03 D5): the claim-guarded form of the loops the static worker
// runs boot-time. Each Sweep try-claims the database's own advisory locks,
// so any number of sweeper replicas cooperate — losers skip cleanly.
type Maintenance struct {
	relay        *Relay // nil without a publisher: events stay in the outbox
	relayElector *Elector
	docs         *DocStore
	docElector   *Elector
}

// NewMaintenance builds the maintenance surface for one tenant database.
// docs MUST be the SAME DocStore the serving plane uses (archive-enabled
// when history offload is configured) — minting a fresh a.Documents() here
// would silently skip history sealing on compaction. pub may be nil (no
// event transport): the relay phase is skipped and outbox rows accumulate.
func NewMaintenance(a *Adapter, reg *registry.Registry, pub event.Publisher, docs *DocStore) *Maintenance {
	m := &Maintenance{
		docs:       docs,
		docElector: NewElector(a, LockKeyDocumentPlane),
	}
	if pub != nil {
		m.relay = NewRelay(a, reg, pub)
		m.relayElector = NewElector(a, LockKeyRelay)
	}
	return m
}

// Sweep implements sweep.Maintainer: one maintenance pass over this
// database — relay the outbox, materialize quiet documents, and (when
// compact is set) compact due document logs. Lock losers report
// Claimed=false with whatever the won phases did.
func (m *Maintenance) Sweep(ctx context.Context, compact bool) (sweep.Result, error) {
	var res sweep.Result

	if m.relay != nil {
		led, err := m.relayElector.TryLead(ctx, func(c context.Context) error {
			n, derr := m.relay.DrainAll(c)
			res.Relayed = n
			return derr
		})
		if err != nil {
			return res, err
		}
		res.Claimed = res.Claimed || led
	}

	led, err := m.docElector.TryLead(ctx, func(c context.Context) error {
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
