//go:build integration

package forgeext_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/grove/crdt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/migrations"
)

const workerCompactTable = "ds_worker_compact_notes"

// TestWorker_DocumentPlaneCompacts proves the worker's document-plane loop
// compacts on its own: a document pushed past its entity's SnapshotEvery
// budget gets its log folded into a snapshot and trimmed with no explicit
// Compact call — and, with history archiving on, the trimmed range is sealed
// into a blob segment, which also proves the loop runs against the
// archive-wired store (a bare Postgres.Documents() would drop the blob
// handle and silently skip sealing).
func TestWorker_DocumentPlaneCompacts(t *testing.T) {
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)

	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatalf("open orchestrator: %v", err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = closeFn()

	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "wnote",
		Kind: registry.KindDocument,
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt", SnapshotEvery: 3, QuietWindow: 50 * time.Millisecond},
		Schema: &registry.DynamicSchema{
			Table:   workerCompactTable,
			Columns: []registry.DynamicColumn{{Name: "title", Type: registry.ColText}},
		},
	})

	// The dynamic entity table is DDL — create it as owner before the
	// restricted app role runs the plane. The owner adapter also serves as
	// the RLS-bypassing lens on the physical CRDT tables below.
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	ent, ok := reg.Get("wnote")
	if !ok {
		t.Fatal("entity 'wnote' not registered")
	}
	if err := owner.EnsureDynamic(ctx, ent); err != nil {
		t.Fatalf("EnsureDynamic: %v", err)
	}

	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)

	ext := forgeext.New(reg,
		forgeext.WithConfig(fabriq.Config{
			Postgres: fabriq.PostgresConfig{DSN: appDSN},
			Redis:    fabriq.RedisConfig{Addr: redisAddr},
			Storage: fabriq.StorageConfig{
				StorageDriver: "file://" + filepath.Join(t.TempDir(), "blobs"),
				DefaultBucket: "fabriq-test",
			},
			Documents: fabriq.DocumentsConfig{ArchiveHistory: true},
		}),
		forgeext.WithWorker(true),
		// The production compaction cadence is 30s; the test drives it fast
		// so the sweep fires well inside the wait deadline.
		forgeext.WithDocCompactInterval(2*time.Second),
	)

	app := forge.NewApp(forge.AppConfig{
		Name:        "fabriq-worker-compact-test",
		HTTPAddress: ":0",
	})
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := ext.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = ext.Shutdown(shutdownCtx)
		_ = ext.Stop(context.Background())
	})
	if err := ext.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := ext.Fabriq()
	if f == nil {
		t.Fatal("ext.Fabriq() returned nil after Start")
	}
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("tenant.WithTenant: %v", err)
	}

	docID := "wnote/" + event.NewID()
	for i := 0; i < 3; i++ {
		raw, merr := json.Marshal(fmt.Sprintf("v%d", i))
		if merr != nil {
			t.Fatal(merr)
		}
		update, merr := json.Marshal([]crdt.ChangeRecord{{
			Table: workerCompactTable, PK: docID, Field: "title", CRDTType: crdt.TypeLWW,
			HLC: crdt.HLC{Timestamp: int64(100 + i), NodeID: "n1"}, NodeID: "n1", Value: raw,
		}})
		if merr != nil {
			t.Fatal(merr)
		}
		if err := f.Document().ApplyUpdate(tctx, docID, update); err != nil {
			t.Fatalf("ApplyUpdate #%d: %v", i, err)
		}
	}

	// The document-plane loop ticks every second; poll the physical tables
	// (as owner, bypassing RLS) until the sweep has compacted the doc.
	count := func(table string) int {
		rows, qerr := owner.Driver().Query(context.Background(),
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE doc_id = $1`, table), docID)
		if qerr != nil {
			t.Fatalf("count %s: %v", table, qerr)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("count %s: no row", table)
		}
		var n int
		if serr := rows.Scan(&n); serr != nil {
			t.Fatalf("scan count: %v", serr)
		}
		return n
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if count("fabriq_crdt_snapshots") == 1 && count("fabriq_crdt_updates") == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker never compacted %s: %d snapshot rows, %d update rows",
				docID, count("fabriq_crdt_snapshots"), count("fabriq_crdt_updates"))
		}
		time.Sleep(200 * time.Millisecond)
	}

	// ArchiveHistory=true: the trimmed range must have been sealed to a blob
	// segment — proof the worker compacts through the archive-wired store.
	if got := count("fabriq_crdt_segments"); got != 1 {
		t.Fatalf("fabriq_crdt_segments has %d rows for %s, want 1 (history sealed by worker compaction)", got, docID)
	}
}
