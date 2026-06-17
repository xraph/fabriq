package trovestore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/xraph/trove"
	"github.com/xraph/trove/drivers/memdriver"

	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/fabriqerr"
)

func newMemAdapter(t *testing.T) *trovestore.Adapter {
	t.Helper()
	ctx := context.Background()
	drv := memdriver.New()
	if err := drv.Open(ctx, "mem://"); err != nil {
		t.Fatal(err)
	}
	tr, err := trove.Open(drv, trove.WithDefaultBucket("test"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.CreateBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	return trovestore.New(tr, "test")
}

func TestTroveAdapterRoundTrip(t *testing.T) {
	a := newMemAdapter(t)
	ctx := context.Background()
	if _, err := a.Put(ctx, "k1", bytes.NewReader([]byte("hello")), blob.PutOpts{ContentType: "text/plain", Size: 5}); err != nil {
		t.Fatal(err)
	}
	rc, info, err := a.Get(ctx, "k1")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "hello" || info.Size != 5 {
		t.Fatalf("roundtrip mismatch: %q size=%d", got, info.Size)
	}
	if _, err := a.Head(ctx, "missing"); !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
