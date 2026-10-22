package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// TestClient_WithTenant: the copy sends another X-Tenant-ID with the same key,
// and the original keeps its own tenant.
func TestClient_WithTenant(t *testing.T) {
	type seen struct{ tenant, auth string }
	var (
		mu  sync.Mutex
		got []seen
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, seen{r.Header.Get("X-Tenant-ID"), r.Header.Get("Authorization")})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	orig := testClient(t, srv, "acme")
	beta := orig.WithTenant("beta")
	if beta == orig {
		t.Fatal("WithTenant returned the receiver, want a copy")
	}

	ctx := context.Background()
	if err := orig.do(ctx, http.MethodGet, "/ping", nil, nil, nil); err != nil {
		t.Fatalf("original do: %v", err)
	}
	if err := beta.do(ctx, http.MethodGet, "/ping", nil, nil, nil); err != nil {
		t.Fatalf("copy do: %v", err)
	}

	want := []seen{{"acme", "Bearer fq_testkey"}, {"beta", "Bearer fq_testkey"}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("requests = %+v, want %+v", got, want)
	}
}
