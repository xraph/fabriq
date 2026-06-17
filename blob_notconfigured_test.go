package fabriq_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestBlobNotConfigured(t *testing.T) {
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	w := fabriqtest.NewWorld(reg)
	f, err := fabriq.New(reg, fabriq.Ports{Store: w.Store, Relational: w.Rel})
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := tenant.WithTenant(context.Background(), "t1")
	if _, err := f.Blob().Head(ctx, "any/key"); !errors.Is(err, fabriq.ErrStoreNotConfigured) {
		t.Fatalf("want ErrStoreNotConfigured, got %v", err)
	}
}
