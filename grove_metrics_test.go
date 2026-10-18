package fabriq

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	gometrics "github.com/xraph/go-utils/metrics"
	"github.com/xraph/grove"
	"github.com/xraph/grove/observability"

	"github.com/xraph/fabriq/core/registry"
)

// labelRecorder stands in for forge's metrics: it records the label set of
// every histogram grove asks for.
type labelRecorder struct {
	gometrics.Metrics
	mu     sync.Mutex
	labels []map[string]string
}

func (r *labelRecorder) Histogram(name string, opts ...gometrics.MetricOption) gometrics.Histogram {
	var o gometrics.MetricOptions
	for _, opt := range opts {
		opt(&o)
	}
	r.mu.Lock()
	r.labels = append(r.labels, o.Labels)
	r.mu.Unlock()
	return gometrics.NewMockHistogram()
}

func (r *labelRecorder) collections() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[string]bool{}
	for _, l := range r.labels {
		seen[l[collectionLabel]] = true
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

type gmSite struct {
	grove.BaseModel `grove:"table:gm_sites"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Name            string `grove:"name"`
}

// grove labels each query's duration with the first table in its SQL. Three
// kinds of statement would mint a new label value per tenant or per caller:
// schema-isolation scans written tenant_<id>.fabriq_*, runtime dynamic entity
// tables, and raw analytics SQL a tenant writes. Through the sink fabriq
// hands grove, the label set stays fixed no matter how many of each run.
func TestBoundCollectionsKeepsTheLabelSetFixed(t *testing.T) {
	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{Name: "site", Kind: registry.KindAggregate, Model: (*gmSite)(nil)})
	if err := reg.Validate(); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		name := fmt.Sprintf("widget%d", i)
		reg.MustRegister(registry.EntitySpec{Name: name, Kind: registry.KindAggregate,
			Schema: &registry.DynamicSchema{Table: "ds_" + name,
				Columns: []registry.DynamicColumn{{Name: "colour", Type: registry.ColText}}}})
	}

	rec := &labelRecorder{Metrics: gometrics.NewMockMetrics()}
	tel := observability.NewTelemetry("postgresql")
	tel.SetMetrics(boundCollections(rec, reg))
	run := func(query string) {
		_, op := tel.StartSQL(context.Background(), query)
		op.End(nil)
	}

	for i := range 40 {
		// (a) schema isolation: every tenant's sweep scans its own schema.
		run(fmt.Sprintf(`SELECT doc_id, tenant_id, entity FROM tenant_t%d.fabriq_crdt_docs WHERE flagged = FALSE`, i))
		run(fmt.Sprintf(`UPDATE "tenant_t%d"."fabriq_outbox" SET published_at = now() WHERE id = $1`, i))
		// (b) runtime dynamic entities.
		run(fmt.Sprintf(`SELECT * FROM ds_widget%d WHERE id = $1`, i))
		// (c) raw analytics SQL a tenant writes.
		run(fmt.Sprintf(`SELECT * FROM x_%08x`, i*7919))
		run(fmt.Sprintf(`WITH q%d AS (SELECT 1) SELECT * FROM q%d`, i, i))
	}
	// Static entity tables and system tables keep their names.
	run(`SELECT id, name FROM gm_sites WHERE id = $1`)
	run(`INSERT INTO fabriq_insights_rollup_state (metric) VALUES ($1)`)
	run(`COMMIT`)

	got := strings.Join(rec.collections(), ",")
	want := ",fabriq_crdt_docs,fabriq_insights_rollup_state,fabriq_outbox,gm_sites,other"
	if got != want {
		t.Fatalf("collection label values = %q, want %q", got, want)
	}
	for _, l := range rec.labels {
		if l["db.system.name"] != "postgresql" {
			t.Fatalf("the other labels must pass through untouched: %v", l)
		}
	}
}

func TestBoundCollectionsPassesOtherMetricsThrough(t *testing.T) {
	rec := &labelRecorder{Metrics: gometrics.NewMockMetrics()}
	m := boundCollections(rec, registry.New())
	m.Histogram("some.other.histogram", gometrics.WithLabel(collectionLabel, "tenant_x.anything"))
	if got := rec.labels[0][collectionLabel]; got != "tenant_x.anything" {
		t.Fatalf("a histogram other than grove's was rewritten: %q", got)
	}
	if boundCollections(nil, registry.New()) != nil {
		t.Fatal("a nil sink must stay nil")
	}
}

// systemTables must name every fixed table fabriq creates, or that table's
// queries would be reported as "other".
func TestSystemTablesCoverEveryCreatedTable(t *testing.T) {
	known := map[string]bool{}
	for _, s := range systemTables {
		known[s] = true
	}
	created := regexp.MustCompile(`CREATE TABLE IF NOT EXISTS ([a-z_][a-z0-9_]*)\s*(\(|PARTITION)`)
	for _, dir := range []string{"migrations", "adapters"} {
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			src, rerr := os.ReadFile(path) // #nosec G304 -- walking the repo's own source tree in a test
			if rerr != nil {
				return rerr
			}
			for _, m := range created.FindAllStringSubmatch(string(src), -1) {
				if !known[m[1]] {
					t.Errorf("%s creates %s, which systemTables does not list", path, m[1])
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
