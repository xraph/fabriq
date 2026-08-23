package modguard

import (
	"os/exec"
	"strings"
	"testing"
)

// forbidden are module path prefixes that must never appear in core's build
// graph. Core is the port layer: a consumer importing it wants entity specs
// and interfaces, not a database client.
var forbidden = []string{
	"github.com/ClickHouse/",
	"github.com/elastic/",
	"github.com/jackc/pgx",
	"github.com/redis/go-redis",
	"github.com/xraph/trove",
	"github.com/duckdb/",
	"github.com/testcontainers/",
	"github.com/FalkorDB",
	"github.com/paulmach/orb",
	"k8s.io/",
}

// TestCoreHasNoDriverDependencies is the fence. Before core was its own
// module nothing stopped a driver import from landing here and spreading to
// every consumer without a single test going red. Now something does.
func TestCoreHasNoDriverDependencies(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "./...")
	cmd.Dir = "../.." // the core module root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}

	var bad []string
	for _, pkg := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		for _, f := range forbidden {
			if strings.HasPrefix(pkg, f) {
				bad = append(bad, pkg)
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("core's build graph contains driver packages: %v\n"+
			"core is the port layer. Put the adapter in the root module instead.", bad)
	}
}
