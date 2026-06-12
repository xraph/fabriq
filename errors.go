package fabriq

import (
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
)

// Sentinel errors form fabriq's typed taxonomy. Callers branch with
// errors.Is; rich variants (VersionConflictError, NotFoundError) carry
// detail and still match their sentinel. The canonical values live in core
// packages (root imports core, never the reverse) and are aliased here so
// application code uniformly writes fabriq.ErrX.
var (
	// ErrNoTenant is returned for any fabric call whose context was not
	// stamped with a tenant by auth middleware.
	ErrNoTenant = tenant.ErrNoTenant

	// ErrTenantHookTripped is returned when the grove pre-query backstop
	// detects a relational query without a tenant predicate. It firing
	// means a bug in fabriq itself: the structural stamping was bypassed.
	ErrTenantHookTripped = tenant.ErrTenantHookTripped

	// ErrNotFound is returned when an aggregate or row does not exist
	// within the calling tenant's scope.
	ErrNotFound = fabriqerr.ErrNotFound

	// ErrVersionConflict is returned when a command's expected version
	// does not match the stored aggregate version.
	ErrVersionConflict = fabriqerr.ErrVersionConflict

	// ErrProjectionLag is returned by WaitForProjection when the deadline
	// expires before the projection catches up to the requested version.
	ErrProjectionLag = fabriqerr.ErrProjectionLag

	// ErrStoreNotConfigured is returned by capability ports whose backing
	// store was not configured on Open (e.g. Graph() without FalkorDB).
	ErrStoreNotConfigured = fabriqerr.ErrStoreNotConfigured
)

// VersionConflictError reports an optimistic-concurrency failure.
type VersionConflictError = fabriqerr.VersionConflictError

// NotFoundError reports a missing aggregate within the tenant's scope.
type NotFoundError = fabriqerr.NotFoundError
