package fabriq

import (
	"errors"
	"fmt"
	"testing"
)

func TestSentinelErrors_AreDistinct(t *testing.T) {
	sentinels := []error{
		ErrNoTenant,
		ErrNotFound,
		ErrVersionConflict,
		ErrProjectionLag,
		ErrTenantHookTripped,
		ErrStoreNotConfigured,
	}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i != j && errors.Is(a, b) {
				t.Errorf("sentinel %v unexpectedly Is %v", a, b)
			}
		}
	}
}

func TestVersionConflictError_IsErrVersionConflict(t *testing.T) {
	err := &VersionConflictError{Aggregate: "asset", AggID: "01H", Expected: 3, Actual: 5}

	if !errors.Is(err, ErrVersionConflict) {
		t.Fatal("VersionConflictError must satisfy errors.Is(err, ErrVersionConflict)")
	}
	wrapped := fmt.Errorf("exec: %w", err)
	if !errors.Is(wrapped, ErrVersionConflict) {
		t.Fatal("wrapped VersionConflictError must still match ErrVersionConflict")
	}
	var vc *VersionConflictError
	if !errors.As(wrapped, &vc) {
		t.Fatal("errors.As must recover *VersionConflictError")
	}
	if vc.Expected != 3 || vc.Actual != 5 {
		t.Fatalf("recovered conflict carries wrong versions: %+v", vc)
	}
}

func TestVersionConflictError_Message(t *testing.T) {
	err := &VersionConflictError{Aggregate: "asset", AggID: "01H", Expected: 3, Actual: 5}
	want := `fabriq: version conflict on asset/01H: expected 3, actual 5`
	if err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestNotFoundError_CarriesEntityAndID(t *testing.T) {
	err := &NotFoundError{Entity: "site", ID: "01J"}
	if !errors.Is(err, ErrNotFound) {
		t.Fatal("NotFoundError must satisfy errors.Is(err, ErrNotFound)")
	}
	want := `fabriq: site "01J" not found`
	if err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
}
