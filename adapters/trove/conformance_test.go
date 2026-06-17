package trovestore_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/xraph/trove"
	"github.com/xraph/trove/drivers/memdriver"

	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/conformance"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

type troveMemBackend struct {
	reg *registry.Registry
	n   atomic.Int64
}

func (b *troveMemBackend) Name() string { return "trove-mem" }

func (b *troveMemBackend) Capabilities() conformance.CapabilitySet {
	// memdriver supports no presign/multipart/range; those cases skip.
	return conformance.CapabilitySet{}
}

func (b *troveMemBackend) Setup(t *testing.T) *conformance.Env {
	t.Helper()
	ctx := context.Background()
	drv := memdriver.New()
	if err := drv.Open(ctx, "mem://"); err != nil {
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
	primary, _ := tenant.WithTenant(context.Background(), fmt.Sprintf("conf%d-a", n))
	foreign, _ := tenant.WithTenant(context.Background(), fmt.Sprintf("conf%d-b", n))
	return &conformance.Env{
		Ctx:        primary,
		ForeignCtx: foreign,
		Registry:   b.reg,
		Blob:       trovestore.New(tr, "conf"),
	}
}

func TestBlobConformanceTroveMem(t *testing.T) {
	reg := registry.New()
	conformance.RunBlob(t, &troveMemBackend{reg: reg})
}
