// Package catalog is the control plane for database-per-tenant fabriq
// (catalog mode; spec 2026-07-03, D1/D2): a small, strongly-consistent
// registry mapping each tenant to the cluster and database that own its
// source of truth.
//
// The catalog exists ONLY in catalog mode. Hash-sharded and single-shard
// deployments keep fabriq's zero-catalog property: minting a tenant is its
// first write. In catalog mode that trade is reversed on purpose —
// dedicated databases require provisioning, and provisioning requires a
// place to record it.
//
// The catalog is deliberately tiny: Get on the request path (cached by the
// shard directory), Put with optimistic concurrency for the provisioning
// state machine, List for the sweeper and the fleet migration roller.
package catalog

import (
	"context"
	"time"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// State is a tenant's lifecycle state in the catalog.
type State string

const (
	// StatePending — row exists, nothing provisioned yet.
	StatePending State = "pending"
	// StateCreating — CREATE DATABASE issued.
	StateCreating State = "creating"
	// StateMigrating — database exists, migration chain running.
	StateMigrating State = "migrating"
	// StateActive — routable; the only state the directory serves.
	StateActive State = "active"
	// StateSuspended — deliberately routed off (offboarding, incident).
	StateSuspended State = "suspended"
	// StateFailed — provisioning failed; listable for operators, never
	// routable. The provisioner may resume it.
	StateFailed State = "failed"
)

// Valid reports whether s is a known lifecycle state.
func (s State) Valid() bool {
	switch s {
	case StatePending, StateCreating, StateMigrating, StateActive, StateSuspended, StateFailed:
		return true
	}
	return false
}

// Entry is one tenant's catalog row.
type Entry struct {
	TenantID  string `json:"tenantId"`
	ClusterID string `json:"clusterId"`
	Database  string `json:"database"`
	State     State  `json:"state"`
	// Version is the fabriq migration version the tenant database was last
	// observed at (grove migration versions, e.g. "202607030030"). The
	// router fails closed when it is below the binary's floor.
	Version string `json:"version"`
	// UpdatedAt is the optimistic-concurrency token: Put succeeds only when
	// it matches the stored row (zero UpdatedAt = create, must not exist).
	// The store stamps a fresh UpdatedAt on every successful write.
	UpdatedAt time.Time `json:"updatedAt"`
}

// ShardID derives the routing shard id for this entry: one tenant database
// = one shard, addressed as "{clusterId}/{database}".
func (e Entry) ShardID() string { return e.ClusterID + "/" + e.Database }

// Cursor is an opaque List pagination token ("" = first page).
type Cursor string

// Catalog is the control-plane port. Implementations: the Postgres control
// database (adapters/postgres) and fabriqtest.FakeCatalog. Both are gated
// by the contract suite in catalogtest.
type Catalog interface {
	// Get returns a tenant's entry, or CodeNotFound.
	Get(ctx context.Context, tenantID string) (Entry, error)

	// Put writes an entry with optimistic concurrency on UpdatedAt:
	// a zero UpdatedAt creates (CodeAlreadyExists if present); a non-zero
	// UpdatedAt updates iff it matches the stored token
	// (CodeVersionConflict otherwise). Invalid entries are CodeInvalidInput.
	Put(ctx context.Context, e Entry) (Entry, error)

	// List pages through every entry in stable (tenant id) order.
	List(ctx context.Context, cursor Cursor, limit int) ([]Entry, Cursor, error)
}

// ValidateEntry checks the invariants every store enforces before writing.
func ValidateEntry(e Entry) error {
	if e.TenantID == "" || e.ClusterID == "" || e.Database == "" {
		return fabriqerr.New(fabriqerr.CodeInvalidInput,
			"catalog entries require tenantId, clusterId and database.")
	}
	if !e.State.Valid() {
		return fabriqerr.New(fabriqerr.CodeInvalidInput,
			"unknown catalog state.",
			fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{"state": string(e.State)}}))
	}
	return nil
}
