package fabriqtest_test

import (
	"context"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

type nfDoc struct {
	grove.BaseModel `grove:"table:nfdocs"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Title           string `grove:"title,notnull"`
}

func TestNewFabric_ExecThenGet(t *testing.T) {
	r := registry.New()
	r.MustRegister(registry.EntitySpec{Name: "nfdoc", Kind: registry.KindAggregate, Model: (*nfDoc)(nil)})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	fab := fabriqtest.NewFabric(fabriqtest.NewWorld(r))
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	res, err := fab.Exec(ctx, command.Command{Entity: "nfdoc", Op: command.OpCreate, Payload: &nfDoc{Title: "hi"}})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	var got nfDoc
	if err := fab.Relational().Get(ctx, "nfdoc", res.AggID, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Title != "hi" {
		t.Fatalf("want hi, got %q", got.Title)
	}
}
