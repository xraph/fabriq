package pathctx

import (
	"context"
	"testing"
)

func TestWithSearchPath_RoundTrips(t *testing.T) {
	ctx, err := WithSearchPath(context.Background(), "tenant_acme")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got := SchemaOrEmpty(ctx); got != "tenant_acme" {
		t.Fatalf("got %q, want tenant_acme", got)
	}
}

func TestSchemaOrEmpty_AbsentIsEmpty(t *testing.T) {
	if got := SchemaOrEmpty(context.Background()); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestWithSearchPath_RejectsInvalid(t *testing.T) {
	for _, bad := range []string{"", "public", "tenant_ACME", "tenant_a-b", "drop_table", "tenant_" + longName(60)} {
		if _, err := WithSearchPath(context.Background(), bad); err == nil {
			t.Fatalf("expected rejection of %q", bad)
		}
	}
}

func longName(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a'
	}
	return string(b)
}
