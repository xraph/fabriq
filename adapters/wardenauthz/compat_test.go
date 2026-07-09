package wardenauthz_test

import (
	"github.com/xraph/fabriq/adapters/wardenauthz"
	"github.com/xraph/fabriq/forgeext/adminapi"
)

// Compile-time guarantee that the adapter satisfies fabriq's Authorizer port.
// This is the ONLY fabriq reference in the module (test-scoped; go.mod replaces
// fabriq with ../..).
var _ adminapi.Authorizer = (*wardenauthz.Authorizer)(nil)
