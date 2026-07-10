package main

import (
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/analytics"
)

// The customer AnalyticsSpec is the trust-boundary showcase: name/tier/country
// cross intact, email is pseudonymized (salted hash), and any unlisted field is
// stripped. This pins that contract to core/analytics.Redact so a spec drift is
// a failing test.
func TestCustomerAnalyticsSpec_Redaction(t *testing.T) {
	spec := customerSpec().Analytics
	if spec == nil {
		t.Fatal("customerSpec() has no AnalyticsSpec")
	}
	const salt = "test-salt"
	raw := json.RawMessage(`{"name":"Ada","email":"ada@example.com","tier":"gold","country":"US","password":"hunter2"}`)

	out, err := analytics.Redact(raw, spec, salt)
	if err != nil {
		t.Fatalf("Redact: %v", err)
	}
	var got map[string]any
	if err = json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal redacted: %v", err)
	}

	// Allow-listed fields survive verbatim.
	for k, want := range map[string]string{"name": "Ada", "tier": "gold", "country": "US"} {
		if got[k] != want {
			t.Errorf("field %q = %v, want %q", k, got[k], want)
		}
	}
	// Unlisted field is stripped (deny-by-default).
	if _, ok := got["password"]; ok {
		t.Error("unlisted field \"password\" leaked past redaction")
	}
	// email is pseudonymized: present, a 64-char hex string, and NOT the raw value.
	h, ok := got["email"].(string)
	if !ok {
		t.Fatalf("email = %v (%T), want hashed string", got["email"], got["email"])
	}
	if h == "ada@example.com" {
		t.Error("email was not hashed (raw value crossed the boundary)")
	}
	if len(h) != 64 {
		t.Errorf("email hash = %q (len %d), want 64-char sha256 hex", h, len(h))
	}

	// Stable: equal input hashes equally (enables cross-tenant count-distinct/join).
	out2, err := analytics.Redact(raw, spec, salt)
	if err != nil {
		t.Fatalf("Redact (2nd): %v", err)
	}
	if string(out) != string(out2) {
		t.Error("redaction is not deterministic for equal input")
	}
}
