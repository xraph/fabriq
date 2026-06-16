package fabriqtest_test

import (
	"testing"

	"github.com/xraph/fabriq/conformance"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestConformance_Fake is the fast (Docker-free) conformance gate: the fakes
// must satisfy every universal case and correctly degrade on the ones that
// require capabilities they lack. It runs in `make test`.
func TestConformance_Fake(t *testing.T) {
	conformance.RunAll(t, fabriqtest.NewConformanceBackend(t))
}
