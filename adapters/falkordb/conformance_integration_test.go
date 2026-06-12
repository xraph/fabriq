//go:build integration

package falkordb_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/xraph/fabriq/adapters/falkordb"
	"github.com/xraph/fabriq/adapters/graphtest"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestFalkorDBConformance runs the exported openCypher conformance suite
// against a real FalkorDB — the gate any graph engine must pass before
// fabriq projects into it.
func TestFalkorDBConformance(t *testing.T) {
	addr := fabriqtest.StartFalkorDB(t)
	reg := registry.New() // suite passes explicit targets; registry only needed for hydration

	var caseN atomic.Int64
	graphtest.Run(t, func(t *testing.T) graphtest.Harness {
		target := fmt.Sprintf("conformance_%d", caseN.Add(1))
		ctx, err := tenant.WithTenant(context.Background(), "conf")
		if err != nil {
			t.Fatal(err)
		}
		a, err := falkordb.Open(ctx, falkordb.Config{Addr: addr}, reg, nil,
			falkordb.WithLiveTargetResolver(func(context.Context, string) (string, error) {
				return target, nil // each case reads the graph it seeded
			}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = a.Close() })
		return graphtest.Harness{Graph: a, Target: target, Ctx: ctx}
	})
}
