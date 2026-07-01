// Package command is fabriq's only write path for KindAggregate entities.
//
// Exec runs spec-driven validation, then — inside one tenant-stamped
// transaction — writes the aggregate row and appends exactly one versioned
// event to the transactional outbox. The Store/Tx ports are implemented by
// adapters/postgres (grove) in production and by fabriqtest fakes in unit
// tests; no engine types appear here.
package command

import (
	"context"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
)

// Op is the command operation.
type Op int

const (
	// OpCreate inserts a new aggregate (version 1).
	OpCreate Op = iota
	// OpUpdate replaces an existing aggregate's row (version+1).
	OpUpdate
	// OpDelete removes the row; the deletion is still a versioned event.
	OpDelete
	// OpUpsert inserts when the aggregate is absent (version 1, "created")
	// or updates when present (version+1, "updated"); idempotent by AggID.
	OpUpsert
)

// Verb returns the event verb for the operation.
func (o Op) Verb() string {
	switch o {
	case OpCreate:
		return registry.VerbCreated
	case OpUpdate:
		return registry.VerbUpdated
	case OpDelete:
		return registry.VerbDeleted
	case OpUpsert:
		return "upsert" // error/log label only; the event verb is resolved at apply time
	default:
		return "unknown"
	}
}

// Command describes one write.
type Command struct {
	// Entity is the registry name, e.g. "asset".
	Entity string

	// Op selects create/update/delete.
	Op Op

	// AggID identifies the aggregate. Required for update/delete/upsert;
	// optional for create (a ULID is minted when empty).
	AggID string

	// Payload is the entity's grove model instance for create/update. The
	// structural columns (id, tenant_id, version) are stamped by the
	// executor — caller-provided values are ignored for id/version and
	// rejected for a foreign tenant_id.
	Payload any

	// ExpectedVersion enables optimistic concurrency: when set, the stored
	// version must match or the command fails with a VersionConflictError.
	ExpectedVersion *int64

	// SkipTypeCheck bypasses application-level payload type validation for this
	// one write (dynamic entities only). Zero value (false) = validate.
	SkipTypeCheck bool
}

// Result reports the outcome of one command.
type Result struct {
	AggID   string
	Version int64
	EventID string
}

// Tx is the unit-of-work surface a store exposes to the executor. All
// methods run inside the same database transaction, already scoped to the
// calling tenant.
type Tx interface {
	// CurrentVersion returns the aggregate's stored version, 0 if absent.
	CurrentVersion(ctx context.Context, ent *registry.Entity, aggID string) (int64, error)

	// ApplyChange writes the row: insert/update with the column-keyed
	// values (already structurally stamped), or delete for OpDelete.
	ApplyChange(ctx context.Context, ent *registry.Entity, op Op, aggID string, version int64, vals map[string]any) error

	// AppendOutbox appends the event envelope to the transactional outbox.
	AppendOutbox(ctx context.Context, env event.Envelope) error

	// Exec runs a raw statement inside the command transaction (the tenant is
	// already stamped via SET LOCAL app.tenant_id). It is the escape hatch a
	// LifecycleHook uses to write its own rows atomically with the aggregate
	// change. SQL here is control-plane-trusted — the same level as a migration
	// or the relational raw Query escape hatch; the command path is Postgres-
	// bound (the source of truth), so this introduces no engine type into core.
	Exec(ctx context.Context, sql string, args ...any) error
}

// Store opens tenant-scoped units of work. Implementations must reject
// unstamped contexts and stamp the tenant into the transaction
// (SET LOCAL app.tenant_id for Postgres + RLS).
type Store interface {
	InTenantTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}
