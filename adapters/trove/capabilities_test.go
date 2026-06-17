package trovestore_test

import (
	"testing"

	"github.com/xraph/fabriq/core/blob"
)

func TestMemAdapterReportsNoOptionalCaps(t *testing.T) {
	a := newMemAdapter(t) // memdriver has no presign/multipart/range
	caps := a.Capabilities()
	if caps.Presign || caps.Multipart || caps.Range {
		t.Fatalf("memdriver caps should all be false, got %+v", caps)
	}
	// When a capability is absent, the typed method returns ErrUnsupported.
	if _, ok := interface{}(a).(blob.Presigner); !ok {
		t.Fatal("Adapter should structurally satisfy Presigner (gated by Caps)")
	}
}
