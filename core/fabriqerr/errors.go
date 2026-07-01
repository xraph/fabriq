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

// Code is a stable, machine-readable error category. Every driver and layer
// maps its failures into one of these; HTTP and RPC boundaries switch on Code
// rather than matching on error-message substrings.
type Code string

const (
	CodeNotFound            Code = "not_found"
	CodeAlreadyExists       Code = "already_exists"
	CodeConstraintViolation Code = "constraint_violation"
	CodeSchemaMismatch      Code = "schema_mismatch"
	CodeInvalidInput        Code = "invalid_input"
	CodeVersionConflict     Code = "version_conflict"
	CodeUnauthorized        Code = "unauthorized"
	CodePermissionDenied    Code = "permission_denied"
	CodeUnavailable         Code = "unavailable"
	CodeTimeout             Code = "timeout"
	CodeSerialization       Code = "serialization"
	CodeNotConfigured       Code = "not_configured"
	CodeInternal            Code = "internal"
)

// Meta carries structured, driver-native detail. Fields are decomposed (never a
// raw driver dump); Detail holds driver-specific extras with no typed slot.
type Meta struct {
	Driver     string            `json:"driver,omitempty"`
	SQLState   string            `json:"sqlstate,omitempty"`
	Constraint string            `json:"constraint,omitempty"`
	Table      string            `json:"table,omitempty"`
	Column     string            `json:"column,omitempty"`
	Detail     map[string]string `json:"detail,omitempty"`
}

// Error is fabriq's canonical structured error. Message is always safe to show a
// caller and never contains driver text; the underlying driver error is kept as
// cause (reachable via errors.As / Unwrap) but never serialized as-is.
type Error struct {
	Code      Code   `json:"code"`
	Message   string `json:"message"`
	Entity    string `json:"entity,omitempty"`
	ID        string `json:"id,omitempty"`
	Op        string `json:"op,omitempty"`
	Meta      Meta   `json:"meta,omitempty"`
	Retryable bool   `json:"retryable"`
	cause     error
}

func (e *Error) Error() string {
	if e.Message != "" {
		return "fabriq: " + string(e.Code) + ": " + e.Message
	}
	return "fabriq: " + string(e.Code)
}

// Unwrap exposes the wrapped driver cause for logging and errors.As, without
// ever placing it in the caller-facing message.
func (e *Error) Unwrap() error { return e.cause }

// Is maps the structured Code back onto the package sentinels so existing
// errors.Is(err, ErrNotFound) / ErrVersionConflict / ErrStoreNotConfigured
// checks keep working.
func (e *Error) Is(target error) bool {
	switch target {
	case ErrNotFound:
		return e.Code == CodeNotFound
	case ErrVersionConflict:
		return e.Code == CodeVersionConflict
	case ErrStoreNotConfigured:
		return e.Code == CodeNotConfigured
	default:
		return false
	}
}

// Option configures an Error at construction.
type Option func(*Error)

func WithEntity(entity, id string) Option { return func(e *Error) { e.Entity, e.ID = entity, id } }
func WithOp(op string) Option             { return func(e *Error) { e.Op = op } }
func WithMeta(m Meta) Option              { return func(e *Error) { e.Meta = m } }
func WithRetryable(r bool) Option         { return func(e *Error) { e.Retryable = r } }
func WithCause(err error) Option          { return func(e *Error) { e.cause = err } }

// New builds a structured error with no wrapped cause.
func New(code Code, msg string, opts ...Option) *Error {
	e := &Error{Code: code, Message: msg}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Wrap builds a structured error carrying cause (reachable via Unwrap).
func Wrap(code Code, cause error, msg string, opts ...Option) *Error {
	e := New(code, msg, opts...)
	e.cause = cause
	return e
}

// SafeMessage returns the default caller-facing message for a Code. Callers that
// have nothing more specific to say pass this to New/Wrap.
func SafeMessage(code Code) string {
	switch code {
	case CodeNotFound:
		return "The requested resource was not found."
	case CodeAlreadyExists:
		return "A resource with the same identity already exists."
	case CodeConstraintViolation:
		return "The request violates a data constraint."
	case CodeSchemaMismatch:
		return "The requested entity type is not available."
	case CodeInvalidInput:
		return "The request was invalid."
	case CodeVersionConflict:
		return "The resource was modified concurrently; retry with the current version."
	case CodeUnauthorized:
		return "Authentication is required."
	case CodePermissionDenied:
		return "You do not have permission to perform this action."
	case CodeUnavailable:
		return "The backing store is temporarily unavailable."
	case CodeTimeout:
		return "The operation timed out."
	case CodeSerialization:
		return "The transaction could not be serialized; retry."
	case CodeNotConfigured:
		return "This capability is not configured."
	default:
		return "An internal error occurred."
	}
}
