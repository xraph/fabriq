//go:build integration

package fabriq_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"testing"
	"time"

	"github.com/xraph/grove/crdt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// lwwUpdate encodes one LWW field write as a grove-crdt update blob.
func lwwUpdate(t *testing.T, docID, field string, value any, hlcWall int64, node string) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := json.Marshal([]crdt.ChangeRecord{{
		Table: "pages", PK: docID, Field: field, CRDTType: crdt.TypeLWW,
		HLC: crdt.HLC{Timestamp: hlcWall, NodeID: node}, NodeID: node, Value: raw,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return blob
}

func docE2E(t *testing.T) (*fabriq.Fabriq, *fabriq.Stores, *registry.Registry) {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()

	// The demo "pages" materialization target is no longer part of the shipped
	// migration chain (it would collide with a host's own tables); create it as
	// owner before provisioning the app role.
	fabriqtest.ApplyDDL(t, superDSN, domain.PagesDDL())

	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres:      fabriq.PostgresConfig{DSN: appDSN},
		Redis:         fabriq.RedisConfig{Addr: redisAddr},
		Subscriptions: fabriq.SubscriptionsConfig{ConflationWindow: 30 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	relay := postgres.NewRelay(stores.Postgres, reg, stores.Redis, postgres.WithRelayPollInterval(100*time.Millisecond))
	elector := postgres.NewElector(stores.Postgres, 1001, postgres.WithElectorRetry(100*time.Millisecond))
	go func() { _ = elector.Run(runCtx, relay.Run) }()

	return f, stores, reg
}

func TestE2E_DocumentPlane(t *testing.T) {
	f, stores, _ := docE2E(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	docID := "page/" + event.NewID()
	docs := f.Document()

	// Subscribe to the page BEFORE it exists: materialization must make
	// CRDT docs look like perfectly normal entities downstream.
	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()
	deltas, err := f.Subscribe(subCtx, query.SubscribeScope{Entity: "page", Scope: "id", ID: docID})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	// Two "clients" edit concurrently; the later HLC wins the title (LWW).
	if err := docs.ApplyUpdate(ctx, docID, lwwUpdate(t, docID, "title", "Draft", 100, "client-a")); err != nil {
		t.Fatalf("ApplyUpdate: %v", err)
	}
	if err := docs.ApplyUpdate(ctx, docID, lwwUpdate(t, docID, "body", "hello world", 101, "client-b")); err != nil {
		t.Fatal(err)
	}
	if err := docs.ApplyUpdate(ctx, docID, lwwUpdate(t, docID, "title", "Final Title", 200, "client-b")); err != nil {
		t.Fatal(err)
	}
	// A stale concurrent title write (older HLC) must lose the merge.
	if err := docs.ApplyUpdate(ctx, docID, lwwUpdate(t, docID, "title", "STALE", 150, "client-a")); err != nil {
		t.Fatal(err)
	}

	// Merged state before materialization.
	snap, err := docs.Snapshot(ctx, docID)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var merged map[string]any
	if err := json.Unmarshal(snap.Snapshot, &merged); err != nil {
		t.Fatal(err)
	}
	if merged["title"] != "Final Title" || merged["body"] != "hello world" {
		t.Fatalf("merged state = %v", merged)
	}
	if snap.Version != 0 {
		t.Fatalf("not yet materialized, version = %d", snap.Version)
	}

	// Sync from scratch returns every update; resuming returns nothing new.
	blob, err := docs.Sync(ctx, docID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var sp struct {
		Seq     int64             `json:"seq"`
		Updates []json.RawMessage `json:"updates"`
	}
	if err := json.Unmarshal(blob, &sp); err != nil {
		t.Fatal(err)
	}
	if len(sp.Updates) != 4 || sp.Seq == 0 {
		t.Fatalf("sync = seq %d, %d updates", sp.Seq, len(sp.Updates))
	}

	// Quiet window (page spec: 2s) passes -> materialize: ONE ordinary
	// versioned event + the relational row.
	time.Sleep(2500 * time.Millisecond)
	n, err := stores.Postgres.Documents().MaterializeQuiet(context.Background(), nil)
	if err != nil {
		t.Fatalf("MaterializeQuiet: %v", err)
	}
	if n != 1 {
		t.Fatalf("materialized %d docs, want 1", n)
	}

	var page domain.Page
	if err := f.Relational().Get(ctx, "page", docID, &page); err != nil {
		t.Fatalf("page row missing after materialization: %v", err)
	}
	if page.Title != "Final Title" || page.Body != "hello world" || page.Version != 1 {
		t.Fatalf("page = %+v", page)
	}

	// The page.updated delta arrives like any other entity's.
	select {
	case d := <-deltas:
		if d.Type != "page.updated" || d.Version != 1 {
			t.Fatalf("delta = %+v", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no delta from materialization")
	}

	// Idempotent: nothing new to materialize.
	if n, err := stores.Postgres.Documents().MaterializeQuiet(context.Background(), nil); err != nil || n != 0 {
		t.Fatalf("re-materialize = (%d, %v), want (0, nil)", n, err)
	}

	// Compaction folds the log; merged state and sync output are stable.
	if err := docs.Compact(ctx, docID); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	snap2, err := docs.Snapshot(ctx, docID)
	if err != nil {
		t.Fatal(err)
	}
	if string(snap2.Snapshot) != string(snap.Snapshot) {
		t.Fatalf("compaction changed merge results:\n%s\n%s", snap.Snapshot, snap2.Snapshot)
	}
	if snap2.Version != 1 {
		t.Fatalf("post-materialization version = %d", snap2.Version)
	}

	// Post-merge validation: a violating edit flags instead of writing.
	if err := docs.ApplyUpdate(ctx, docID, lwwUpdate(t, docID, "title", "", 300, "client-a")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	n, err = stores.Postgres.Documents().MaterializeQuiet(context.Background(), func(entity string, vals map[string]any) error {
		if s, _ := vals["title"].(string); s == "" {
			return errors.New("title must not be empty")
		}
		return nil
	})
	if err != nil || n != 0 {
		t.Fatalf("violating doc materialized: (%d, %v)", n, err)
	}
	var still domain.Page
	if err := f.Relational().Get(ctx, "page", docID, &still); err != nil || still.Version != 1 {
		t.Fatalf("flagged doc must keep its last materialized row: %+v %v", still, err)
	}
	// And the flag is queryable for resolution tooling.
	var flagged bool
	row := stores.Postgres.Driver().QueryRow(context.Background(),
		`SELECT flagged FROM fabriq_crdt_docs WHERE doc_id = $1`, docID)
	if err := row.Scan(&flagged); err != nil || !flagged {
		t.Fatalf("doc not flagged: %v %v", flagged, err)
	}

}

// TestE2E_DocumentLiveSync proves the live transport: updates fan out on
// the document's RAW channel — every frame, in order, no conflation —
// while the durable log keeps serving Sync for catch-up.
func TestE2E_DocumentLiveSync(t *testing.T) {
	f, _, _ := docE2E(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	docID := "page/" + event.NewID()

	// Two collaborators attach before any edits.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	clientA, err := f.SubscribeDocument(subCtx, docID)
	if err != nil {
		t.Fatalf("SubscribeDocument: %v", err)
	}
	clientB, err := f.SubscribeDocument(subCtx, docID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // pump attach

	// A burst of edits: conflation would collapse these; the raw path
	// must deliver every frame, in order.
	for i := 1; i <= 5; i++ {
		if err := f.Document().ApplyUpdate(ctx, docID,
			lwwUpdate(t, docID, "body", fmt.Sprintf("rev %d", i), int64(100+i), "client-a")); err != nil {
			t.Fatal(err)
		}
	}

	for name, ch := range map[string]<-chan query.Delta{"A": clientA, "B": clientB} {
		var seqs []int64
		deadline := time.After(10 * time.Second)
		for len(seqs) < 5 {
			select {
			case frame := <-ch:
				if frame.Type != "page.sync" || frame.AggID != docID {
					t.Fatalf("client %s got %+v", name, frame)
				}
				var changes []crdt.ChangeRecord
				if err := json.Unmarshal(frame.Payload, &changes); err != nil || len(changes) != 1 {
					t.Fatalf("client %s frame payload is not the update blob: %v", name, err)
				}
				seqs = append(seqs, frame.Version)
			case <-deadline:
				t.Fatalf("client %s received %d/5 frames (conflation must not apply)", name, len(seqs))
			}
		}
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("client %s frames out of order: %v", name, seqs)
			}
		}
	}

	// Cross-tenant subscribers see nothing (channel is tenant-derived).
	rival, _ := tenant.WithTenant(context.Background(), "rival")
	rivalCh, err := f.SubscribeDocument(rival, docID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := f.Document().ApplyUpdate(ctx, docID, lwwUpdate(t, docID, "title", "X", 500, "client-a")); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-rivalCh:
		t.Fatalf("rival tenant received a sync frame: %+v", frame)
	case <-time.After(2 * time.Second):
	}
}
