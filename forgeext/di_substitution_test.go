package forgeext_test

import (
	"testing"

	"github.com/xraph/vessel"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/remote"
)

// TestDISubstitution_RemoteFabricUnderPortKey is the payoff for keying
// dependency injection on query.Fabric instead of the concrete facade.
//
// A consumer resolves the port. Whether the container was handed an embedded
// engine or a client speaking to one over the wire is not its concern, and
// nothing at the call site changes when a deployment switches. This test stands
// in for that consumer: it registers *remote.Fabric under the port key and the
// registry under its name, then resolves exactly the way cortex and weave do.
//
// It lives in forgeext rather than core/query because core is its own module
// and must not depend on remote, which depends on core.
func TestDISubstitution_RemoteFabricUnderPortKey(t *testing.T) {
	rf := remote.New(remote.Loopback{})
	reg := registry.New()

	c := vessel.New()
	if err := vessel.Provide(c, func() (*remote.Fabric, error) { return rf, nil },
		vessel.As(new(query.Fabric))); err != nil {
		t.Fatalf("Provide remote fabric: %v", err)
	}
	if err := vessel.Provide(c, func() (*registry.Registry, error) { return reg, nil },
		vessel.WithName(registry.ServiceName)); err != nil {
		t.Fatalf("Provide registry: %v", err)
	}

	got, err := vessel.Inject[query.Fabric](c)
	if err != nil {
		t.Fatalf("Inject[query.Fabric] against a remote engine: %v", err)
	}
	if got != query.Fabric(rf) {
		t.Fatalf("Inject[query.Fabric] = %v, want the remote fabric %v", got, rf)
	}

	gotReg, err := vessel.InjectNamed[*registry.Registry](c, registry.ServiceName)
	if err != nil {
		t.Fatalf("InjectNamed(%q): %v", registry.ServiceName, err)
	}
	if gotReg != reg {
		t.Fatalf("registry = %v, want %v", gotReg, reg)
	}
}
