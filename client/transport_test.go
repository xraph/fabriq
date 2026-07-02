package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func testClient(t *testing.T, srv *httptest.Server, tenant string) *Client {
	t.Helper()
	dsn := "fabriq://fq_testkey@" + srv.Listener.Addr().String() + "?tls=false&version=3"
	if tenant != "" {
		dsn = "fabriq://fq_testkey@" + srv.Listener.Addr().String() + "/" + tenant + "?tls=false&version=3"
	}
	c, err := Connect(context.Background(), dsn, WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatalf("Connect() unexpected error: %v", err)
	}
	return c
}

func TestClient_Do_SendsHeadersMethodPathQueryBodyAndDecodes(t *testing.T) {
	type reqPayload struct {
		Name string `json:"name"`
	}
	type respPayload struct {
		ID string `json:"id"`
	}

	var gotMethod, gotPath, gotAuth, gotTenant, gotVersion, gotQuery string
	var gotBody reqPayload

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotTenant = r.Header.Get("X-Tenant-ID")
		gotVersion = r.Header.Get("X-Fabriq-Api-Version")
		gotQuery = r.URL.Query().Get("filter")

		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(respPayload{ID: "abc123"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "acme")

	query := url.Values{"filter": []string{"active"}}
	var out respPayload
	err := c.do(context.Background(), http.MethodPost, "/entities/widget", query, reqPayload{Name: "gizmo"}, &out)
	if err != nil {
		t.Fatalf("do() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/entities/widget" {
		t.Errorf("path = %q, want /admin/entities/widget", gotPath)
	}
	if gotAuth != "Bearer fq_testkey" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer fq_testkey")
	}
	if gotTenant != "acme" {
		t.Errorf("X-Tenant-ID header = %q, want %q", gotTenant, "acme")
	}
	if gotVersion != "3" {
		t.Errorf("X-Fabriq-Api-Version header = %q, want %q", gotVersion, "3")
	}
	if gotQuery != "active" {
		t.Errorf("query param filter = %q, want %q", gotQuery, "active")
	}
	if gotBody.Name != "gizmo" {
		t.Errorf("request body name = %q, want %q", gotBody.Name, "gizmo")
	}
	if out.ID != "abc123" {
		t.Errorf("decoded out.ID = %q, want %q", out.ID, "abc123")
	}
}

func TestClient_Do_OmitsTenantHeaderWhenUnset(t *testing.T) {
	var sawTenantHeader bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Tenant-ID") != "" {
			sawTenantHeader = true
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := testClient(t, srv, "")
	if err := c.do(context.Background(), http.MethodGet, "/ping", nil, nil, nil); err != nil {
		t.Fatalf("do() unexpected error: %v", err)
	}
	if sawTenantHeader {
		t.Errorf("expected no X-Tenant-ID header when tenant is unset")
	}
}

// TestClient_Do_ErrorShapes table-drives the several distinct error JSON
// shapes the adminapi can emit, proving do() parses defensively and keys
// off the real HTTP status rather than trusting the body.
func TestClient_Do_ErrorShapes(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantStatus  int
		wantMessage string
	}{
		{
			name:        "renderError nested error object",
			status:      http.StatusNotFound,
			body:        `{"error":{"code":"not_found","message":"nope","retryable":false}}`,
			wantStatus:  http.StatusNotFound,
			wantMessage: "nope",
		},
		{
			name:        "forge.InternalError/BadRequest details shape",
			status:      http.StatusInternalServerError,
			body:        `{"code":500,"details":"boom","error":""}`,
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "boom",
		},
		{
			name:        "auth deny middleware string error",
			status:      http.StatusForbidden,
			body:        `{"error":"access denied","code":"Forbidden"}`,
			wantStatus:  http.StatusForbidden,
			wantMessage: "access denied",
		},
		{
			name:        "unparseable body falls back to raw",
			status:      http.StatusBadGateway,
			body:        `not even json`,
			wantStatus:  http.StatusBadGateway,
			wantMessage: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			c := testClient(t, srv, "")
			err := c.do(context.Background(), http.MethodGet, "/whatever", nil, nil, nil)
			if err == nil {
				t.Fatalf("do() expected error, got nil")
			}

			apiErr, ok := err.(*APIError)
			if !ok {
				t.Fatalf("do() error type = %T, want *APIError", err)
			}
			if apiErr.Status != tt.wantStatus {
				t.Errorf("Status = %d, want %d", apiErr.Status, tt.wantStatus)
			}
			if tt.wantMessage != "" && apiErr.Message != tt.wantMessage {
				t.Errorf("Message = %q, want %q", apiErr.Message, tt.wantMessage)
			}
			if apiErr.Body != tt.body {
				t.Errorf("Body = %q, want raw body %q", apiErr.Body, tt.body)
			}
			if apiErr.Error() == "" {
				t.Errorf("Error() returned empty string")
			}
		})
	}
}

func TestConnect_UnsupportedTransport(t *testing.T) {
	_, err := Connect(context.Background(), "fabriq+grpc://fq_k@h.co")
	if err == nil {
		t.Fatalf("Connect() expected error for grpc transport, got nil")
	}
}

func TestConnect_InvalidDSN(t *testing.T) {
	_, err := Connect(context.Background(), "not-a-dsn")
	if err == nil {
		t.Fatalf("Connect() expected error for invalid dsn, got nil")
	}
}

func TestConnect_DefaultsAndOptions(t *testing.T) {
	c, err := Connect(context.Background(), "fabriq://fq_k@localhost:9999/acme?tls=false&version=2")
	if err != nil {
		t.Fatalf("Connect() unexpected error: %v", err)
	}
	if c.baseURL != "http://localhost:9999/admin" {
		t.Errorf("baseURL = %q, want %q", c.baseURL, "http://localhost:9999/admin")
	}
	if c.key != "fq_k" {
		t.Errorf("key = %q, want %q", c.key, "fq_k")
	}
	if c.tenant != "acme" {
		t.Errorf("tenant = %q, want %q", c.tenant, "acme")
	}
	if c.version != 2 {
		t.Errorf("version = %d, want 2", c.version)
	}
	if c.hc == nil {
		t.Errorf("hc should default to a non-nil http.Client")
	}
}

func TestWithHTTPClient_NilIsNoop(t *testing.T) {
	c, err := Connect(context.Background(), "fabriq://fq_k@localhost:9999", WithHTTPClient(nil))
	if err != nil {
		t.Fatalf("Connect() unexpected error: %v", err)
	}
	if c.hc == nil {
		t.Errorf("hc should not be nil after WithHTTPClient(nil)")
	}
}
