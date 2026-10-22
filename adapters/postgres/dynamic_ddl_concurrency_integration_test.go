//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestDynamicDDL_ConcurrentEnsureSameTable runs EnsureDynamic for the same new
// table from two adapters (two processes booting together) at once. Both must
// succeed, and the table must end up with exactly one tenant_isolation policy
// and RLS enabled and forced.
func TestDynamicDDL_ConcurrentEnsureSameTable(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	const rounds = 8
	reg := registry.New()
	for i := range rounds {
		reg.MustRegister(registry.EntitySpec{
			Name: fmt.Sprintf("race%d", i),
			Schema: &registry.DynamicSchema{
				Table: fmt.Sprintf("ds_race%d", i),
				Columns: []registry.DynamicColumn{
					{Name: "sku", Type: registry.ColText},
					{Name: "qty", Type: registry.ColInt},
				},
				Indexes: []registry.DynamicIndex{
					{Name: fmt.Sprintf("ds_race%d_sku_idx", i), Columns: []string{"sku"}},
				},
			},
		})
	}

	a1, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("open a1: %v", err)
	}
	t.Cleanup(func() { _ = a1.Close() })
	orch, err := migrations.NewOrchestrator(a1.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	a2, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("open a2: %v", err)
	}
	t.Cleanup(func() { _ = a2.Close() })

	for i := range rounds {
		ent, ok := reg.Get(fmt.Sprintf("race%d", i))
		if !ok {
			t.Fatalf("race%d not registered", i)
		}
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for j, a := range []*postgres.Adapter{a1, a2} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[j] = a.EnsureDynamic(ctx, ent)
			}()
		}
		wg.Wait()
		for j, e := range errs {
			if e != nil {
				t.Fatalf("round %d: EnsureDynamic from adapter %d: %v", i, j+1, e)
			}
		}

		table := ent.Spec.Schema.Table
		var policies int
		if err := a1.Driver().QueryRow(ctx,
			`SELECT count(*) FROM pg_policies WHERE tablename = $1 AND policyname = 'tenant_isolation'`,
			table).Scan(&policies); err != nil {
			t.Fatalf("count policies: %v", err)
		}
		if policies != 1 {
			t.Errorf("%s: tenant_isolation policies = %d, want 1", table, policies)
		}
		var enabled, forced bool
		if err := a1.Driver().QueryRow(ctx,
			`SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = to_regclass($1)`,
			table).Scan(&enabled, &forced); err != nil {
			t.Fatalf("read rls flags: %v", err)
		}
		if !enabled || !forced {
			t.Errorf("%s: relrowsecurity=%v relforcerowsecurity=%v, want both true", table, enabled, forced)
		}
	}
}

// TestDynamicDDL_ReensureKeepsPolicyForReaders re-ensures an existing dynamic
// table over and over (every boot, sync pass and reconcile does this) while an
// RLS-bound fabriq_app connection reads a seeded row in a loop. The reader
// must never see an error or a missing row: the policy is never absent.
func TestDynamicDDL_ReensureKeepsPolicyForReaders(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")

	if _, err := h.X.Exec(ctx, command.Command{
		Entity:  "orders",
		Op:      command.OpUpsert,
		AggID:   "seed",
		Payload: map[string]any{"sku": "A1", "qty": int64(1)},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	ent, ok := h.Reg.Get("orders")
	if !ok {
		t.Fatal("orders not registered")
	}

	var stop atomic.Bool
	var reads, failures atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				var row map[string]any
				err := h.A.Get(ctx, "orders", "seed", &row)
				reads.Add(1)
				if err != nil || row["sku"] != "A1" {
					failures.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("err=%v row=%v", err, row))
				}
			}
		}()
	}

	deadline := time.Now().Add(3 * time.Second)
	ensures := 0
	for time.Now().Before(deadline) {
		if err := h.superPG.EnsureDynamic(context.Background(), ent); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("re-ensure %d: %v", ensures, err)
		}
		ensures++
	}
	stop.Store(true)
	wg.Wait()

	if n := failures.Load(); n > 0 {
		t.Fatalf("%d of %d reads failed during %d re-ensures; first: %v", n, reads.Load(), ensures, firstErr.Load())
	}
	if reads.Load() == 0 {
		t.Fatal("reader never ran")
	}
	t.Logf("%d reads clean across %d re-ensures", reads.Load(), ensures)
}

// TestDynamicDDL_ReensureDoesNotDeadlockWriters runs command-plane writes
// (SELECT ... FOR UPDATE, then INSERT or UPDATE, in one transaction) while
// the owner re-ensures the table in a loop. The ensure must take its
// strongest lock first: a SHARE lock from CREATE INDEX IF NOT EXISTS that
// later upgrades to ACCESS EXCLUSIVE for ADD COLUMN deadlocks with a writer
// that holds ROW SHARE and wants ROW EXCLUSIVE (40P01).
func TestDynamicDDL_ReensureDoesNotDeadlockWriters(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")
	ent, ok := h.Reg.Get("orders")
	if !ok {
		t.Fatal("orders not registered")
	}

	var stop atomic.Bool
	var writes, failures atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				_, err := h.X.Exec(ctx, command.Command{
					Entity:  "orders",
					Op:      command.OpUpsert,
					AggID:   fmt.Sprintf("w%d-%d", w, i%8),
					Payload: map[string]any{"sku": "S", "qty": int64(i)},
				})
				writes.Add(1)
				if err != nil {
					failures.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
				}
			}
		}()
	}

	deadline := time.Now().Add(4 * time.Second)
	ensures := 0
	var ensureErr error
	for time.Now().Before(deadline) {
		if ensureErr = h.superPG.EnsureDynamic(context.Background(), ent); ensureErr != nil {
			break
		}
		ensures++
	}
	stop.Store(true)
	wg.Wait()

	if ensureErr != nil {
		t.Fatalf("re-ensure %d: %v", ensures, ensureErr)
	}
	if n := failures.Load(); n > 0 {
		t.Fatalf("%d of %d writes failed during %d re-ensures; first: %v", n, writes.Load(), ensures, firstErr.Load())
	}
	if writes.Load() == 0 {
		t.Fatal("writers never ran")
	}
	t.Logf("%d writes clean across %d re-ensures", writes.Load(), ensures)
}
