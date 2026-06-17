package fabriq

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/cachequery"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

type invalidatorTestAsset struct {
	grove.BaseModel `grove:"table:assets"`
	ID              string `grove:"id,pk"              json:"id"`
	TenantID        string `grove:"tenant_id,notnull"  json:"tenant_id"`
	Version         int64  `grove:"version,notnull"    json:"version"`
}

func TestCacheInvalidatorEvictsChangedRow(t *testing.T) {
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name:  "asset",
		Kind:  registry.KindAggregate,
		Model: invalidatorTestAsset{},
		Cache: &registry.CacheSpec{TTL: time.Minute},
	}); err != nil {
		t.Fatal(err)
	}
	ent, _ := reg.Get("asset")
	fc := fabriqtest.NewFakeCache()
	ks := cachequery.EntityRowKeyspace(ent)

	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	// Pre-cache two rows.
	_ = fc.Set(ctx, ks, "a1", []byte(`{"id":"a1"}`))
	_ = fc.Set(ctx, ks, "a2", []byte(`{"id":"a2"}`))

	inv := newCacheInvalidator(fc)
	inv.AfterCommit(ctx, []command.Change{{
		Entity:   ent,
		Op:       command.OpUpdate,
		Envelope: event.Envelope{TenantID: "acme", Aggregate: "asset", AggID: "a1"},
	}})

	if _, ok, _ := fc.Get(ctx, ks, "a1"); ok {
		t.Fatal("changed row a1 must be evicted")
	}
	if _, ok, _ := fc.Get(ctx, ks, "a2"); !ok {
		t.Fatal("sibling row a2 must stay warm")
	}
}
