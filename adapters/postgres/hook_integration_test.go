//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// hookHarness keeps the owner open (for side-table DDL + RLS-bypassing
// assertions) and exposes the RLS-constrained app adapter as the command store.
type hookHarness struct {
	owner *postgres.Adapter
	app   *postgres.Adapter
	reg   *registry.Registry
}

func newHookHarness(t testing.TB) *hookHarness {
	t.Helper()
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })

	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// A chronicle-style side table, created as owner BEFORE the app role so the
	// GRANT ON ALL TABLES covers it. RLS + tenant_isolation proves the command
	// tx's tenant stamp flows into a hook's own writes.
	for _, stmt := range []string{
		`CREATE TABLE chronicle_audit (
			id TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			entity TEXT NOT NULL,
			agg_id TEXT NOT NULL,
			version BIGINT NOT NULL,
			op TEXT NOT NULL
		)`,
		`ALTER TABLE chronicle_audit ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE chronicle_audit FORCE ROW LEVEL SECURITY`,
		`CREATE POLICY tenant_isolation ON chronicle_audit
			USING (tenant_id = current_setting('app.tenant_id', true))
			WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`,
	} {
		if _, err := owner.Driver().Exec(ctx, stmt); err != nil {
			t.Fatalf("audit ddl: %v\n%s", err, stmt)
		}
	}

	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	app, err := postgres.Open(ctx, appDSN, reg, postgres.WithGuardedTables(domain.ReadingsSeries))
	if err != nil {
		t.Fatalf("postgres.Open (app): %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })

	return &hookHarness{owner: owner, app: app, reg: reg}
}

// count runs SELECT count(*) via the owner (superuser bypasses RLS, sees all).
func (h *hookHarness) count(t testing.TB, table string) int {
	t.Helper()
	rows, err := h.owner.Driver().Query(context.Background(), `SELECT count(*) FROM `+table)
	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	defer rows.Close()
	var n int
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
	}
	return n
}

// auditHook writes one chronicle_audit row per change, atomically via tx.Exec.
func auditHook() command.HookFunc {
	return func(ctx context.Context, tx command.Tx, ch command.Change) error {
		return tx.Exec(ctx,
			`INSERT INTO chronicle_audit (id, tenant_id, entity, agg_id, version, op)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			ch.Envelope.ID, ch.Envelope.TenantID, ch.Envelope.Aggregate,
			ch.Envelope.AggID, ch.Envelope.Version, ch.Op.Verb())
	}
}

func TestPGLifecycleHook_AtomicParticipationAndVeto(t *testing.T) {
	h := newHookHarness(t)
	ctx := tctx(t, "acme")

	// --- Veto: the hook writes its audit row, then aborts. Everything rolls
	// back together — neither the aggregate nor the audit row survives. ---
	vetoErr := errors.New("policy says no")
	veto := command.HookFunc(func(ctx context.Context, tx command.Tx, ch command.Change) error {
		if err := auditHook()(ctx, tx, ch); err != nil {
			return err
		}
		return vetoErr
	})
	xVeto, err := command.NewExecutor(h.reg, h.app, command.WithHooks(veto))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := xVeto.Exec(ctx, command.Command{
		Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "Vetoed"},
	}); !errors.Is(err, vetoErr) {
		t.Fatalf("want veto error, got %v", err)
	}
	if n := h.count(t, "sites"); n != 0 {
		t.Fatalf("after veto: sites count = %d, want 0 (aggregate write must roll back)", n)
	}
	if n := h.count(t, "chronicle_audit"); n != 0 {
		t.Fatalf("after veto: audit count = %d, want 0 (hook write must roll back atomically)", n)
	}

	// --- Participate: a non-vetoing hook commits its audit row atomically
	// with the aggregate change. ---
	xOK, err := command.NewExecutor(h.reg, h.app, command.WithHooks(auditHook()))
	if err != nil {
		t.Fatal(err)
	}
	res, err := xOK.Exec(ctx, command.Command{
		Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "Committed"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if n := h.count(t, "sites"); n != 1 {
		t.Fatalf("sites count = %d, want 1", n)
	}

	type auditRow struct {
		AggID   string
		Version int64
		Op      string
		Tenant  string
	}
	var got auditRow
	rows, err := h.owner.Driver().Query(context.Background(),
		`SELECT agg_id, version, op, tenant_id FROM chronicle_audit`)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
		if err := rows.Scan(&got.AggID, &got.Version, &got.Op, &got.Tenant); err != nil {
			t.Fatal(err)
		}
	}
	if n != 1 {
		t.Fatalf("audit rows = %d, want 1", n)
	}
	if got.AggID != res.AggID || got.Version != 1 || got.Op != "created" || got.Tenant != "acme" {
		t.Fatalf("audit row = %+v, want {%s 1 created acme}", got, res.AggID)
	}
}
