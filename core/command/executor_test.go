package command_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

type cmdSite struct {
	grove.BaseModel `grove:"table:sites"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
	Note     string `grove:"note"`
}

type cmdDoc struct {
	grove.BaseModel `grove:"table:pages"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
}

func cmdRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "site", Kind: registry.KindAggregate, Model: (*cmdSite)(nil), GraphNode: "Site",
	})
	r.MustRegister(registry.EntitySpec{
		Name: "page", Kind: registry.KindDocument, Model: (*cmdDoc)(nil),
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt", SnapshotEvery: 64, QuietWindow: time.Second},
	})
	return r
}

// fakeStore is a transactional in-memory command.Store: changes stage into
// a copy and only merge on commit, so batch atomicity is real.
type fakeStore struct {
	versions map[string]int64          // "entity/id" -> version
	rows     map[string]map[string]any // "entity/id" -> column values
	outbox   []event.Envelope
	failOn   func(env event.Envelope) error // injected outbox failure
	tenants  map[string]string              // "entity/id" -> tenant that owns it
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		versions: map[string]int64{},
		rows:     map[string]map[string]any{},
		tenants:  map[string]string{},
	}
}

type fakeTx struct {
	s        *fakeStore
	tenantID string
	versions map[string]int64
	rows     map[string]map[string]any
	tenants  map[string]string
	outbox   []event.Envelope
}

func (s *fakeStore) InTenantTx(ctx context.Context, fn func(ctx context.Context, tx command.Tx) error) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	tx := &fakeTx{s: s, tenantID: tid,
		versions: map[string]int64{}, rows: map[string]map[string]any{}, tenants: map[string]string{}}
	for k, v := range s.versions {
		tx.versions[k] = v
	}
	for k, v := range s.rows {
		tx.rows[k] = v
	}
	for k, v := range s.tenants {
		tx.tenants[k] = v
	}
	if err := fn(ctx, tx); err != nil {
		return err // staged copy dropped = rollback
	}
	s.versions = tx.versions
	s.rows = tx.rows
	s.tenants = tx.tenants
	s.outbox = append(s.outbox, tx.outbox...)
	return nil
}

func key(entity, id string) string { return entity + "/" + id }

func (t *fakeTx) CurrentVersion(_ context.Context, ent *registry.Entity, aggID string) (int64, error) {
	k := key(ent.Spec.Name, aggID)
	// RLS semantics: rows of other tenants are invisible.
	if owner, ok := t.tenants[k]; ok && owner != t.tenantID {
		return 0, nil
	}
	return t.versions[k], nil
}

func (t *fakeTx) ApplyChange(_ context.Context, ent *registry.Entity, op command.Op, aggID string, version int64, vals map[string]any) error {
	k := key(ent.Spec.Name, aggID)
	switch op {
	case command.OpDelete:
		delete(t.versions, k)
		delete(t.rows, k)
		delete(t.tenants, k)
	default:
		t.versions[k] = version
		t.rows[k] = vals
		t.tenants[k] = t.tenantID
	}
	return nil
}

func (t *fakeTx) AppendOutbox(_ context.Context, env event.Envelope) error {
	if t.s.failOn != nil {
		if err := t.s.failOn(env); err != nil {
			return err
		}
	}
	t.outbox = append(t.outbox, env)
	return nil
}

func newExecutor(t testing.TB, store command.Store) *command.Executor {
	t.Helper()
	x, err := command.NewExecutor(cmdRegistry(t), store)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func acmeCtx(t testing.TB) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestExec_CreateMintsIDVersion1AndOneEvent(t *testing.T) {
	store := newFakeStore()
	x := newExecutor(t, store)

	res, err := x.Exec(acmeCtx(t), command.Command{
		Entity: "site", Op: command.OpCreate,
		Payload: &cmdSite{Name: "Plant A"},
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.AggID == "" || len(res.AggID) != 26 {
		t.Fatalf("create must mint a ULID AggID, got %q", res.AggID)
	}
	if res.Version != 1 {
		t.Fatalf("Version = %d, want 1", res.Version)
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox has %d events, want exactly 1", len(store.outbox))
	}
	env := store.outbox[0]
	if env.Type != "site.created" || env.TenantID != "acme" || env.AggID != res.AggID || env.Version != 1 {
		t.Fatalf("envelope = %+v", env)
	}
	if env.ID != res.EventID {
		t.Fatalf("Result.EventID %q != envelope ID %q", res.EventID, env.ID)
	}
	// Payload is column-keyed and structurally stamped.
	if !strings.Contains(string(env.Payload), `"tenant_id":"acme"`) ||
		!strings.Contains(string(env.Payload), `"version":1`) {
		t.Fatalf("payload not structurally stamped: %s", env.Payload)
	}
	// Stored row carries the stamped values, not whatever the caller sent.
	row := store.rows[key("site", res.AggID)]
	if row["tenant_id"] != "acme" || row["version"] != int64(1) || row["id"] != res.AggID {
		t.Fatalf("stored row not stamped: %v", row)
	}
}

func TestExec_UpdateIncrementsVersion(t *testing.T) {
	store := newFakeStore()
	x := newExecutor(t, store)
	ctx := acmeCtx(t)

	created, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "A"}})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := x.Exec(ctx, command.Command{
		Entity: "site", Op: command.OpUpdate, AggID: created.AggID, Payload: &cmdSite{Name: "B"},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Version != 2 {
		t.Fatalf("Version = %d, want 2", updated.Version)
	}
	if store.outbox[1].Type != "site.updated" || store.outbox[1].Version != 2 {
		t.Fatalf("second envelope = %+v", store.outbox[1])
	}
}

func TestExec_DeleteEmitsDeletedEvent(t *testing.T) {
	store := newFakeStore()
	x := newExecutor(t, store)
	ctx := acmeCtx(t)

	created, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "A"}})
	if err != nil {
		t.Fatal(err)
	}
	res, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpDelete, AggID: created.AggID})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if res.Version != 2 {
		t.Fatalf("delete Version = %d, want 2", res.Version)
	}
	env := store.outbox[1]
	if env.Type != "site.deleted" || string(env.Payload) != "{}" {
		t.Fatalf("delete envelope = %+v payload=%s", env, env.Payload)
	}
	if _, exists := store.rows[key("site", created.AggID)]; exists {
		t.Fatal("row must be gone after delete")
	}
}

func TestExec_NoTenant(t *testing.T) {
	x := newExecutor(t, newFakeStore())
	_, err := x.Exec(context.Background(), command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "A"}})
	if !errors.Is(err, tenant.ErrNoTenant) {
		t.Fatalf("want ErrNoTenant, got %v", err)
	}
}

func TestExec_UnknownEntity(t *testing.T) {
	x := newExecutor(t, newFakeStore())
	_, err := x.Exec(acmeCtx(t), command.Command{Entity: "nope", Op: command.OpCreate, Payload: &cmdSite{}})
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("want unknown-entity error, got %v", err)
	}
}

func TestExec_DocumentKindRejected(t *testing.T) {
	x := newExecutor(t, newFakeStore())
	_, err := x.Exec(acmeCtx(t), command.Command{Entity: "page", Op: command.OpCreate, Payload: &cmdDoc{}})
	if err == nil || !strings.Contains(err.Error(), "document") {
		t.Fatalf("document-kind commands must be rejected, got %v", err)
	}
}

func TestExec_OptimisticConcurrency(t *testing.T) {
	store := newFakeStore()
	x := newExecutor(t, store)
	ctx := acmeCtx(t)

	created, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "A"}})
	if err != nil {
		t.Fatal(err)
	}

	stale := int64(7)
	_, err = x.Exec(ctx, command.Command{
		Entity: "site", Op: command.OpUpdate, AggID: created.AggID,
		Payload: &cmdSite{Name: "B"}, ExpectedVersion: &stale,
	})
	if !errors.Is(err, fabriqerr.ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict, got %v", err)
	}
	var vc *fabriqerr.VersionConflictError
	if !errors.As(err, &vc) || vc.Expected != 7 || vc.Actual != 1 {
		t.Fatalf("conflict detail = %+v", vc)
	}
	// Nothing leaked: no event, no version bump.
	if len(store.outbox) != 1 || store.versions[key("site", created.AggID)] != 1 {
		t.Fatal("failed command must leave no trace")
	}

	right := int64(1)
	if _, err := x.Exec(ctx, command.Command{
		Entity: "site", Op: command.OpUpdate, AggID: created.AggID,
		Payload: &cmdSite{Name: "B"}, ExpectedVersion: &right,
	}); err != nil {
		t.Fatalf("matching expected version must succeed: %v", err)
	}
}

func TestExec_CreateExistingConflicts(t *testing.T) {
	store := newFakeStore()
	x := newExecutor(t, store)
	ctx := acmeCtx(t)

	created, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "A"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = x.Exec(ctx, command.Command{
		Entity: "site", Op: command.OpCreate, AggID: created.AggID, Payload: &cmdSite{Name: "again"},
	})
	if !errors.Is(err, fabriqerr.ErrVersionConflict) {
		t.Fatalf("create-on-existing: want ErrVersionConflict, got %v", err)
	}
}

func TestExec_UpdateMissingIsNotFound(t *testing.T) {
	x := newExecutor(t, newFakeStore())
	_, err := x.Exec(acmeCtx(t), command.Command{
		Entity: "site", Op: command.OpUpdate, AggID: "01HMISSING0000000000000000", Payload: &cmdSite{Name: "B"},
	})
	if !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestExec_PayloadTenantForgeryRejected(t *testing.T) {
	x := newExecutor(t, newFakeStore())
	_, err := x.Exec(acmeCtx(t), command.Command{
		Entity: "site", Op: command.OpCreate,
		Payload: &cmdSite{Name: "A", TenantID: "victim"},
	})
	if err == nil || !strings.Contains(err.Error(), "tenant") {
		t.Fatalf("payload with foreign tenant_id must be rejected, got %v", err)
	}
}

func TestExec_RequiredColumnValidation(t *testing.T) {
	x := newExecutor(t, newFakeStore())
	_, err := x.Exec(acmeCtx(t), command.Command{
		Entity: "site", Op: command.OpCreate, Payload: &cmdSite{}, // name is notnull and empty
	})
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("want required-column error mentioning name, got %v", err)
	}
}

func TestExec_WrongPayloadTypeRejected(t *testing.T) {
	x := newExecutor(t, newFakeStore())
	_, err := x.Exec(acmeCtx(t), command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdDoc{}})
	if err == nil {
		t.Fatal("wrong payload type must be rejected")
	}
}

func TestExecBatch_AtomicAndOrdered(t *testing.T) {
	store := newFakeStore()
	x := newExecutor(t, store)
	ctx := acmeCtx(t)

	results, err := x.ExecBatch(ctx, []command.Command{
		{Entity: "site", Op: command.OpCreate, AggID: "01HSITE0000000000000000001", Payload: &cmdSite{Name: "A"}},
		{Entity: "site", Op: command.OpUpdate, AggID: "01HSITE0000000000000000001", Payload: &cmdSite{Name: "A2"}},
	})
	if err != nil {
		t.Fatalf("ExecBatch: %v", err)
	}
	if len(results) != 2 || results[0].Version != 1 || results[1].Version != 2 {
		t.Fatalf("results = %+v", results)
	}
	if len(store.outbox) != 2 {
		t.Fatalf("outbox = %d events, want 2", len(store.outbox))
	}
}

func TestExecBatch_FailureRollsBackEverything(t *testing.T) {
	store := newFakeStore()
	boom := errors.New("disk full")
	calls := 0
	store.failOn = func(event.Envelope) error {
		calls++
		if calls == 2 {
			return boom
		}
		return nil
	}
	x := newExecutor(t, store)

	_, err := x.ExecBatch(acmeCtx(t), []command.Command{
		{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "A"}},
		{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "B"}},
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want injected failure, got %v", err)
	}
	if len(store.outbox) != 0 || len(store.versions) != 0 {
		t.Fatalf("batch failure must roll back everything: outbox=%d versions=%d",
			len(store.outbox), len(store.versions))
	}
}

func TestExecBatch_Empty(t *testing.T) {
	x := newExecutor(t, newFakeStore())
	results, err := x.ExecBatch(acmeCtx(t), nil)
	if err != nil || len(results) != 0 {
		t.Fatalf("empty batch: results=%v err=%v", results, err)
	}
}

func TestExec_TraceparentPropagates(t *testing.T) {
	store := newFakeStore()
	x, err := command.NewExecutor(cmdRegistry(t), store,
		command.WithTraceparent(func(context.Context) string { return "00-abc-def-01" }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.Exec(acmeCtx(t), command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "A"}}); err != nil {
		t.Fatal(err)
	}
	if store.outbox[0].Traceparent != "00-abc-def-01" {
		t.Fatalf("traceparent = %q", store.outbox[0].Traceparent)
	}
}

func BenchmarkExec_Fake(b *testing.B) {
	store := newFakeStore()
	x, err := command.NewExecutor(cmdRegistry(b), store)
	if err != nil {
		b.Fatal(err)
	}
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "A"}}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecBatch100_Fake(b *testing.B) {
	store := newFakeStore()
	x, err := command.NewExecutor(cmdRegistry(b), store)
	if err != nil {
		b.Fatal(err)
	}
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	cmds := make([]command.Command, 100)
	for i := range cmds {
		cmds[i] = command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: fmt.Sprintf("S%d", i)}}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := x.ExecBatch(ctx, cmds); err != nil {
			b.Fatal(err)
		}
	}
}
