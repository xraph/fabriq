package trovestore_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/core/blob"
)

func TestOpenFromDSNRoundTrip(t *testing.T) {
	ctx := context.Background()
	a, err := trovestore.Open(ctx, trovestore.Config{
		StorageDriver: "mem://",
		DefaultBucket: "phase2",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(ctx) })

	var rc io.ReadCloser
	if _, err = a.Put(ctx, "k", bytes.NewReader([]byte("hi")), blob.PutOpts{Size: 2}); err != nil {
		t.Fatal(err)
	}
	rc, _, err = a.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "hi" {
		t.Fatalf("got %q want hi", got)
	}
}

func TestOpenUnknownDriver(t *testing.T) {
	_, err := trovestore.Open(context.Background(), trovestore.Config{StorageDriver: "bogus://x"})
	if err == nil {
		t.Fatal("expected error for unknown driver scheme")
	}
}
