package adminapi

import (
	"context"
	"testing"
	"time"
)

func TestResolveAuthDefaults(t *testing.T) {
	// No DB reachable → cannot enforce auth; warn, provision nothing.
	if d := resolveAuthDefaults(config{}, false); d.ProvisionKeyStore || d.DefaultLogin || d.Warn == "" {
		t.Fatalf("no-DB: got %+v, want no provision/login + a warning", d)
	}
	// DB + nothing configured → provision + default login.
	if d := resolveAuthDefaults(config{}, true); !d.ProvisionKeyStore || !d.DefaultLogin {
		t.Fatalf("db-default: got %+v, want provision + default login", d)
	}
	// Explicit opt-out → nothing, no warning.
	if d := resolveAuthDefaults(config{authDisabled: true}, true); d.ProvisionKeyStore || d.DefaultLogin || d.Warn != "" {
		t.Fatalf("disabled: got %+v, want all off", d)
	}
	// Host already set a KeyStore → don't provision; don't default login.
	if d := resolveAuthDefaults(config{KeyStore: stubStore{}}, true); d.ProvisionKeyStore || d.DefaultLogin {
		t.Fatalf("explicit-store: got %+v, want no provision/login", d)
	}
	// DB + explicit login user → provision, but preserve creds (no default).
	if d := resolveAuthDefaults(config{AdminLoginUser: "root"}, true); !d.ProvisionKeyStore || d.DefaultLogin {
		t.Fatalf("explicit-login: got %+v, want provision + NO default login", d)
	}
}

// stubStore is a no-op KeyStore for the "explicit store" case.
type stubStore struct{}

func (stubStore) Issue(_ context.Context, _ KeySpec) (IssuedKey, error) {
	return IssuedKey{}, nil
}
func (stubStore) IssueSession(_ context.Context, _ time.Duration) (IssuedKey, error) {
	return IssuedKey{}, nil
}
func (stubStore) Lookup(_ context.Context, _ string) (KeyRecord, bool, error) {
	return KeyRecord{}, false, nil
}
func (stubStore) List(_ context.Context) ([]KeyRecord, error) { return nil, nil }
func (stubStore) Revoke(_ context.Context, _ string) error    { return nil }
