package forgeext_test

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/forgeext"
)

// TestExtension_RemoteHandler_NilBeforeStart proves the remote-protocol handler
// accessor is nil until the facade is opened (Start) — the same lifecycle
// contract as Fabriq()/Stores(). The populated path needs live datastores and is
// covered by integration tests.
func TestExtension_RemoteHandler_NilBeforeStart(t *testing.T) {
	ext := forgeext.New(registry.New())
	if h := ext.RemoteHandler(); h != nil {
		t.Fatalf("RemoteHandler() before Start = %v, want nil", h)
	}
}
