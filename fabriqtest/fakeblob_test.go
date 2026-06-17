package fabriqtest_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestFakeBlobRoundTripAndIsolation(t *testing.T) {
	fb := fabriqtest.NewFakeBlob()
	a, _ := tenant.WithTenant(context.Background(), "tenantA")
	b, _ := tenant.WithTenant(context.Background(), "tenantB")

	if _, err := fb.Put(a, "k1", bytes.NewReader([]byte("hello")), blob.PutOpts{ContentType: "text/plain", Size: 5}); err != nil {
		t.Fatal(err)
	}
	rc, info, err := fb.Get(a, "k1")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "hello" || info.Size != 5 {
		t.Fatalf("roundtrip mismatch: %q size=%d", got, info.Size)
	}
	// Tenant isolation: tenantB cannot see tenantA's key.
	if _, err := fb.Head(b, "k1"); !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("want ErrNotFound across tenants, got %v", err)
	}
}
