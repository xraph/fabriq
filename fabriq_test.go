package fabriq_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

type fSite struct {
	grove.BaseModel `grove:"table:sites"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
}

func fReg(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "site", Kind: registry.KindAggregate, Model: (*fSite)(nil), GraphNode: "Site",
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
	})
	return r
}

func newFabric(t testing.TB, w *fabriqtest.World, opts ...fabriq.Option) *fabriq.Fabriq {
	t.Helper()
	f, err := fabriq.New(w.Registry, fabriq.Ports{
		Store:           w.Store,
		Relational:      w.Rel,
		Graph:           w.Graph,
		Search:          w.Search,
		Timeseries:      w.TS,
		Vector:          w.Vector,
		Documents:       w.Docs,
		ProjectionState: w.Projections,
	}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func fCtx(t testing.TB) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestFabriq_ImplementsFabricInterface(_ *testing.T) {
	var _ query.Fabric = (*fabriq.Fabriq)(nil)
}

func TestFabriq_ExecThenRead(t *testing.T) {
	w := fabriqtest.NewWorld(fReg(t))
	f := newFabric(t, w)
	ctx := fCtx(t)

	res, err := f.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &fSite{Name: "Plant"}})
	if err != nil {
		t.Fatal(err)
	}
	var got fSite
	if err := f.Relational().Get(ctx, "site", res.AggID, &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "Plant" {
		t.Fatalf("got %+v", got)
	}
}

func TestFabriq_UnconfiguredPortsErrorLoudly(t *testing.T) {
	w := fabriqtest.NewWorld(fReg(t))
	f, err := fabriq.New(w.Registry, fabriq.Ports{Store: w.Store, Relational: w.Rel})
	if err != nil {
		t.Fatal(err)
	}
	ctx := fCtx(t)

	if err := f.Graph().Query(ctx, "MATCH (n) RETURN n.id", nil, &[]string{}); !errors.Is(err, fabriq.ErrStoreNotConfigured) {
		t.Fatalf("graph: want ErrStoreNotConfigured, got %v", err)
	}
	var hits []map[string]any
	if err := f.Search().Search(ctx, query.SearchQuery{Entity: "site", Query: "x"}, &hits); !errors.Is(err, fabriq.ErrStoreNotConfigured) {
		t.Fatalf("search: want ErrStoreNotConfigured, got %v", err)
	}
	if err := f.Timeseries().BulkWrite(ctx, "s", nil); !errors.Is(err, fabriq.ErrStoreNotConfigured) {
		t.Fatalf("ts: want ErrStoreNotConfigured, got %v", err)
	}
	if err := f.Vector().Upsert(ctx, "site", "1", nil, nil); !errors.Is(err, fabriq.ErrStoreNotConfigured) {
		t.Fatalf("vector: want ErrStoreNotConfigured, got %v", err)
	}
	if _, err := f.Document().Snapshot(ctx, "d"); !errors.Is(err, fabriq.ErrStoreNotConfigured) {
		t.Fatalf("document: want ErrStoreNotConfigured, got %v", err)
	}
}

func TestFabriq_NewRequiresRegistryStoreRelational(t *testing.T) {
	w := fabriqtest.NewWorld(fReg(t))
	if _, err := fabriq.New(nil, fabriq.Ports{Store: w.Store, Relational: w.Rel}); err == nil {
		t.Fatal("nil registry must fail")
	}
	if _, err := fabriq.New(w.Registry, fabriq.Ports{Relational: w.Rel}); err == nil {
		t.Fatal("missing store must fail")
	}
	if _, err := fabriq.New(w.Registry, fabriq.Ports{Store: w.Store}); err == nil {
		t.Fatal("missing relational must fail")
	}
}

func TestFabriq_SubscribeReceivesPublishedDelta(t *testing.T) {
	w := fabriqtest.NewWorld(fReg(t))
	f := newFabric(t, w, fabriq.WithConflationWindow(10*time.Millisecond))
	defer f.Close()
	ctx, cancel := context.WithCancel(fCtx(t))
	defer cancel()

	ch, err := f.Subscribe(ctx, query.SubscribeScope{Entity: "site", Scope: "id", ID: "S1"})
	if err != nil {
		t.Fatal(err)
	}

	f.Hub().Publish("changes:acme:id:S1", query.Delta{
		StreamID: "1-0", Channel: "changes:acme:id:S1", TenantID: "acme",
		Aggregate: "site", AggID: "S1", Version: 1, Type: "site.updated",
		At: time.Now(), Payload: json.RawMessage(`{}`),
	})

	select {
	case d := <-ch:
		if d.AggID != "S1" {
			t.Fatalf("delta = %+v", d)
		}
	case <-time.After(time.Second):
		t.Fatal("no delta delivered")
	}
}

func TestFabriq_SubscribeDeniedByAuthz(t *testing.T) {
	w := fabriqtest.NewWorld(fReg(t))
	f := newFabric(t, w, fabriq.WithAuthz(func(_ context.Context, _ query.SubscribeScope) error {
		return errors.New("nope")
	}))
	defer f.Close()

	if _, err := f.Subscribe(fCtx(t), query.SubscribeScope{Entity: "site", Scope: "id", ID: "S1"}); err == nil {
		t.Fatal("authz deny must fail Subscribe")
	}
}

func TestFabriq_SubscribeRequiresTenant(t *testing.T) {
	w := fabriqtest.NewWorld(fReg(t))
	f := newFabric(t, w)
	defer f.Close()

	if _, err := f.Subscribe(context.Background(), query.SubscribeScope{Entity: "site", Scope: "id", ID: "S1"}); !errors.Is(err, fabriq.ErrNoTenant) {
		t.Fatalf("want ErrNoTenant, got %v", err)
	}
}

func TestFabriq_WaitForProjection(t *testing.T) {
	w := fabriqtest.NewWorld(fReg(t))
	f := newFabric(t, w, fabriq.WithWaitPollInterval(2*time.Millisecond))
	ctx := fCtx(t)

	// Projection catches up after 20ms.
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = w.Projections.SetApplied(context.Background(), "acme", "graph", "site", "S1", 3)
	}()
	if err := f.WaitForProjection(ctx, "graph", "site", "S1", 3); err != nil {
		t.Fatalf("WaitForProjection: %v", err)
	}

	// Never catches up: deadline -> ErrProjectionLag.
	lagCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	err := f.WaitForProjection(lagCtx, "graph", "site", "S1", 99)
	if !errors.Is(err, fabriq.ErrProjectionLag) {
		t.Fatalf("want ErrProjectionLag, got %v", err)
	}
}

func TestConfig_Validate(t *testing.T) {
	good := fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: "postgres://localhost/fabriq"},
		Redis:    fabriq.RedisConfig{Addr: "localhost:6379"},
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	noPG := good
	noPG.Postgres.DSN = ""
	if err := noPG.Validate(); err == nil {
		t.Fatal("missing postgres DSN must fail (postgres is the source of truth)")
	}

	graphNoFalkor := good
	graphNoFalkor.Projections.Graph = true
	if err := graphNoFalkor.Validate(); err == nil {
		t.Fatal("graph projection without falkordb address must fail")
	}

	searchNoES := good
	searchNoES.Projections.Search = true
	if err := searchNoES.Validate(); err == nil {
		t.Fatal("search projection without elasticsearch addresses must fail")
	}
}

func TestFabriq_RepoFor_Typed(t *testing.T) {
	w := fabriqtest.NewWorld(fReg(t))
	f := newFabric(t, w)
	ctx := fCtx(t)

	res, err := f.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &fSite{Name: "Plant A"}})
	if err != nil {
		t.Fatal(err)
	}

	repo, err := fabriq.RepoFor[fSite](f)
	if err != nil {
		t.Fatalf("RepoFor[fSite]: %v", err)
	}
	if repo.Entity() != "site" {
		t.Fatalf("entity = %q", repo.Entity())
	}

	site, err := repo.Get(ctx, res.AggID) // *fSite, typed
	if err != nil {
		t.Fatal(err)
	}
	if site.Name != "Plant A" {
		t.Fatalf("Get = %+v", site)
	}

	one, err := repo.One(ctx, query.ListQuery{Where: []query.Cond{query.Eq("name", "Plant A")}})
	if err != nil || one.ID != res.AggID {
		t.Fatalf("One = (%+v, %v)", one, err)
	}
}
