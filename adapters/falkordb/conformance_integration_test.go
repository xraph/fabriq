//go:build integration

package falkordb_test

import (
	"testing"
)

// TODO(phase 4): run the exported conformance suite against a
// falkordb/falkordb testcontainer once Query/ApplyMutations execute:
//
//	func TestFalkorDBConformance(t *testing.T) {
//	    graphtest.Run(t, func(t *testing.T) graphtest.Harness {
//	        addr := startFalkorDB(t) // testcontainers: falkordb/falkordb:latest
//	        a, err := falkordb.Open(ctx, falkordb.Config{Addr: addr}, reg, world.Rel)
//	        ...
//	        return graphtest.Harness{Graph: a, Target: registry.GraphName("acme"), Ctx: acmeCtx}
//	    })
//	}
//
// The suite (adapters/graphtest) and the dialect translation (mutate.go)
// are already built and unit-tested; this file is the wiring that turns
// them into the engine-swap gate.
func TestFalkorDBConformance_Pending(t *testing.T) {
	t.Skip("phase 4: falkordb Query/ApplyMutations execution pending — see package doc")
}
