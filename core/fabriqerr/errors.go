// Package fabriqerr holds the canonical shared error values used across
// fabriq's core packages. The root fabriq package aliases everything here
// (fabriq.ErrNotFound, fabriq.VersionConflictError, ...) — application code
// should depend on those aliases; core and adapter packages depend on this
// leaf so the dependency direction stays root -> core.
package fabriqerr

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound: aggregate or row absent within the tenant's scope.
	ErrNotFound = errors.New("fabriq: not found")

	// ErrVersionConflict: optimistic-concurrency mismatch.
	ErrVersionConflict = errors.New("fabriq: version conflict")

	// ErrProjectionLag: WaitForProjection deadline expired.
	ErrProjectionLag = errors.New("fabriq: projection lagging")

	// ErrStoreNotConfigured: capability port without a configured backend.
	ErrStoreNotConfigured = errors.New("fabriq: store not configured")

	// ErrQueryTimeout is returned when a query exceeds its time budget — the
	// statement_timeout fires (pg SQLSTATE 57014) or the context deadline is hit.
	ErrQueryTimeout = errors.New("fabriq: query exceeded the time limit")
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
