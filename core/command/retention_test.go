package command_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/tenant"
)

func TestErasureRequestRequiresExactScope(t *testing.T) {
	base := tenant.MustWithTenant(context.Background(), "acme")
	ctx := tenant.MustWithScope(base, "project")
	good := command.ErasureRequest{Entity: "notes", AggregateID: "stable", ScopeID: "project", ExpectedVersion: 2, ReasonCode: "retention_expired"}
	if err := good.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*command.ErasureRequest){
		"broader scope":     func(r *command.ErasureRequest) { r.ScopeID = "" },
		"foreign scope":     func(r *command.ErasureRequest) { r.ScopeID = "foreign" },
		"negative revision": func(r *command.ErasureRequest) { r.ExpectedVersion = -1 },
		"missing identity":  func(r *command.ErasureRequest) { r.AggregateID = "" },
		"content reason":    func(r *command.ErasureRequest) { r.ReasonCode = "private deleted source text" },
		"invalid entity":    func(r *command.ErasureRequest) { r.Entity = "notes;DELETE" },
	} {
		t.Run(name, func(t *testing.T) {
			request := good
			mutate(&request)
			if err := request.Validate(ctx); err == nil {
				t.Fatal("accepted invalid erasure")
			}
		})
	}
	if err := good.Validate(context.Background()); err == nil {
		t.Fatal("accepted missing tenant")
	}
	if err := good.Validate(base); err == nil {
		t.Fatal("accepted unscoped widening")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := good.Validate(canceled); err == nil {
		t.Fatal("accepted canceled request")
	}
}
