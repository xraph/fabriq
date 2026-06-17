//go:build integration

package trovestore_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/xraph/trove"
	"github.com/xraph/trove/drivers/localdriver"

	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/conformance"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

type troveLocalBackend struct {
	reg *registry.Registry
	n   atomic.Int64
}

func (b *troveLocalBackend) Name() string { return "trove-local" }

func (b *troveLocalBackend) Capabilities() conformance.CapabilitySet {
	// localdriver has no presign/multipart/range support; those cases skip.
	return conformance.CapabilitySet{}
}

func (b *troveLocalBackend) Setup(t *testing.T) *conformance.Env {
	t.Helper()
	ctx := context.Background()
	dsn := "file://" + t.TempDir()
	drv := localdriver.New()
	if err := drv.Open(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	tr, err := trove.Open(drv, trove.WithDefaultBucket("conf"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.CreateBucket(ctx, "conf"); err != nil {
		t.Fatal(err)
	}
	n := b.n.Add(1)
	primary, _ := tenant.WithTenant(context.Background(), fmt.Sprintf("local%d-a", n))
	foreign, _ := tenant.WithTenant(context.Background(), fmt.Sprintf("local%d-b", n))
	return &conformance.Env{
		Ctx:        primary,
		ForeignCtx: foreign,
		Registry:   b.reg,
		Blob:       trovestore.New(tr, "conf"),
	}
}

func TestBlobConformanceTroveLocal(t *testing.T) {
	reg := registry.New()
	conformance.RunBlob(t, &troveLocalBackend{reg: reg})
}
