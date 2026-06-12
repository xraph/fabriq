package fabriq

import (
	"errors"
	"fmt"
)

// Sentinel errors form fabriq's typed taxonomy. Callers branch with
// errors.Is; rich variants (VersionConflictError, NotFoundError) carry
// detail and still match their sentinel.
var (
	// ErrNoTenant is returned for any fabric call whose context was not
	// stamped with a tenant by auth middleware.
	ErrNoTenant = errors.New("fabriq: no tenant in context")

	// ErrNotFound is returned when an aggregate or row does not exist
	// within the calling tenant's scope.
	ErrNotFound = errors.New("fabriq: not found")

	// ErrVersionConflict is returned when a command's expected version
	// does not match the stored aggregate version.
	ErrVersionConflict = errors.New("fabriq: version conflict")

	// ErrProjectionLag is returned by WaitForProjection when the deadline
	// expires before the projection catches up to the requested version.
	ErrProjectionLag = errors.New("fabriq: projection lagging")

	// ErrTenantHookTripped is returned when the grove pre-query backstop
	// detects a relational query without a tenant predicate. It firing
	// means a bug in fabriq itself: the structural stamping was bypassed.
	ErrTenantHookTripped = errors.New("fabriq: tenant guard tripped")

	// ErrStoreNotConfigured is returned by capability ports whose backing
	// store was not configured on Open (e.g. Graph() without FalkorDB).
	ErrStoreNotConfigured = errors.New("fabriq: store not configured")
)

// VersionConflictError reports an optimistic-concurrency failure.
type VersionConflictError struct {
	Aggregate string
	AggID     string
	Expected  int64
	Actual    int64
}

func (e *VersionConflictError) Error() string {
	return fmt.Sprintf("fabriq: version conflict on %s/%s: expected %d, actual %d",
		e.Aggregate, e.AggID, e.Expected, e.Actual)
}

// Is makes errors.Is(err, ErrVersionConflict) match.
func (e *VersionConflictError) Is(target error) bool { return target == ErrVersionConflict }

// NotFoundError reports a missing aggregate within the tenant's scope.
type NotFoundError struct {
	Entity string
	ID     string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("fabriq: %s %q not found", e.Entity, e.ID)
}

// Is makes errors.Is(err, ErrNotFound) match.
func (e *NotFoundError) Is(target error) bool { return target == ErrNotFound }
