package cachequery_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/cachequery"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

type row struct {
	grove.BaseModel `grove:"table:assets"`
	ID              string `grove:"id,pk" json:"id"`
	TenantID        string `grove:"tenant_id,notnull" json:"tenant_id"`
	Version         int64  `grove:"version,notnull" json:"version"`
	Name            string `grove:"name" json:"name"`
}

// fakeRel is a minimal RelationalQuerier recording calls and returning canned rows.
type fakeRel struct {
	gets    int
	getMany int
	rows    map[string]row // id -> row
}

func (f *fakeRel) Get(_ context.Context, _ string, id string, into any) error {
	f.gets++
	r, ok := f.rows[id]
	if !ok {
		return fabriqerr.ErrNotFound
	}
	*(into.(*row)) = r
	return nil
}

func (f *fakeRel) GetMany(_ context.Context, _ string, ids []string, into any) error {
	f.getMany++
	out := into.(*[]row)
	for _, id := range ids {
		if r, ok := f.rows[id]; ok {
			*out = append(*out, r)
		}
	}
	return nil
}

func (f *fakeRel) List(context.Context, string, query.ListQuery, any) error { return nil }
func (f *fakeRel) Query(context.Context, any, string, ...any) error         { return nil }

func reg(t *testing.T, cached bool) *registry.Registry {
	t.Helper()
	r := registry.New()
	spec := registry.EntitySpec{Name: "asset", Kind: registry.KindAggregate, Model: row{}}
	if cached {
		spec.Cache = &registry.CacheSpec{TTL: time.Minute}
	}
	if err := r.Register(spec); err != nil {
		t.Fatal(err)
	}
	return r
}

func tctx(t *testing.T) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestCachedGet_CachesAndServesWarm(t *testing.T) {
	fr := &fakeRel{rows: map[string]row{"a1": {ID: "a1", Name: "Pump"}}}
	cr := cachequery.New(fr, fabriqtest.NewFakeCache(), reg(t, true))
	ctx := tctx(t)

	var got row
	if err := cr.Get(ctx, "asset", "a1", &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "Pump" || fr.gets != 1 {
		t.Fatalf("first get: row=%+v gets=%d", got, fr.gets)
	}
	// Second get is served from cache: underlying not called again.
	var got2 row
	if err := cr.Get(ctx, "asset", "a1", &got2); err != nil {
		t.Fatal(err)
	}
	if got2.Name != "Pump" || fr.gets != 1 {
		t.Fatalf("second get should hit cache: row=%+v gets=%d (want 1)", got2, fr.gets)
	}
}

func TestCachedGet_PassThroughWhenNotOptedIn(t *testing.T) {
	fr := &fakeRel{rows: map[string]row{"a1": {ID: "a1", Name: "Pump"}}}
	cr := cachequery.New(fr, fabriqtest.NewFakeCache(), reg(t, false)) // not cached
	ctx := tctx(t)
	var got row
	_ = cr.Get(ctx, "asset", "a1", &got)
	_ = cr.Get(ctx, "asset", "a1", &got)
	if fr.gets != 2 {
		t.Fatalf("non-opted entity must pass through every call: gets=%d (want 2)", fr.gets)
	}
}
