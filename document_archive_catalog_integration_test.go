//go:build integration

package fabriq_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/grove/crdt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/sweep"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// With archiving on, the sweeper's compaction seals trimmed CRDT history to
// the blob plane and trims the tenant's OWN update log — proving the
// archive-enabled DocStore reaches the maintenance path (not a fresh
// non-archive one).
func TestCatalogMode_DocumentArchivePerTenant(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	tmp := t.TempDir()

	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	if _, err := p.Provision(ctx, "acme", "c1"); err != nil {
		t.Fatal(err)
	}
	_ = cat.Close()
	tenantDSN, _ := ops.TenantDSN("c1", "fabriq_acme")
	fabriqtest.ApplyDDL(t, tenantDSN, cmDDL())

	// A CRDT entity with a tiny SnapshotEvery so a few updates trigger
	// compaction; archiving requested globally.
	reg := cmRegistryArchive(t) // cmnote with SnapshotEvery:2, QuietWindow:0
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog:   fabriq.CatalogConfig{DSN: dsn, ClusterDSNs: map[string]string{"c1": dsn}, AllowSuperuser: true},
		Redis:     fabriq.RedisConfig{Addr: redisAddr},
		Storage:   fabriq.StorageConfig{StorageDriver: "file://" + tmp, DefaultBucket: "docs"},
		Documents: fabriq.DocumentsConfig{ArchiveHistory: true},
	})
	if err != nil {
		t.Fatalf("catalog mode must accept archiving: %v", err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	tctx, _ := tenant.WithTenant(ctx, "acme")
	docID := "cmnote/01ARCHIVE00000000000000001"
	for i := 1; i <= 6; i++ {
		upd, _ := json.Marshal([]crdt.ChangeRecord{{
			Table: "cm_notes", PK: docID, Field: "title", CRDTType: crdt.TypeLWW,
			HLC: crdt.HLC{Timestamp: int64(i), NodeID: "n1"}, NodeID: "n1",
			Value: json.RawMessage(`"v` + string(rune('0'+i)) + `"`),
		}})
		if err := f.Document().ApplyUpdate(tctx, docID, upd); err != nil {
			t.Fatalf("ApplyUpdate %d: %v", i, err)
		}
	}

	// One sweep pass: compaction seals sealed segments and trims the log.
	eng := sweep.New(stores.Catalog, stores.TenantSweeper(), sweep.Config{
		CompactEvery: 1, MinVersion: migrations.HeadVersion(),
		OnError: func(tid string, e error) { t.Errorf("sweep %s: %v", tid, e) },
	})
	eng.Pass(ctx)

	// A sealed segment row exists in the tenant's OWN database — history was
	// offloaded, not left inline. (Segment metadata table lives per tenant.)
	segs := fabriqtest.QueryStrings(t, tenantDSN,
		`SELECT doc_id FROM fabriq_crdt_segments WHERE doc_id = '`+docID+`'`)
	if len(segs) == 0 {
		t.Fatal("no sealed history segment — archive not wired into the maintenance DocStore")
	}
}
