package fabriqtest

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestFakeCAS_StoreDedupAndRetrieve(t *testing.T) {
	cas := NewFakeCAS()
	ctx := context.Background()
	h1, _, err := cas.Store(ctx, strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	h2, _, _ := cas.Store(ctx, strings.NewReader("hello"))
	if h1 != h2 {
		t.Fatal("identical content must hash identically")
	}
	if cas.Len() != 1 {
		t.Fatalf("dedup failed: %d blobs", cas.Len())
	}
	rc, err := cas.Retrieve(ctx, h1)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	if string(b) != "hello" {
		t.Fatalf("retrieve got %q", b)
	}
}
