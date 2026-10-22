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

func TestResolveAuthDefaults_RequiredWithoutDB_Errors(t *testing.T) {
	var cfg config
	WithAuthRequired()(&cfg)
	d := resolveAuthDefaults(cfg, false)
	if d.Err == nil {
		t.Fatalf("required without DB: got %+v, want an error", d)
	}
	if got, want := d.Err.Error(), "adminapi: auth is required but no Postgres DB is reachable"; got != want {
		t.Errorf("err = %q, want %q", got, want)
	}
	if d.Warn != "" || d.ProvisionKeyStore || d.DefaultLogin {
		t.Errorf("required without DB: got %+v, want only the error", d)
	}

	// An explicit KeyStore satisfies the requirement without a DB.
	cfg.KeyStore = stubStore{}
	if d := resolveAuthDefaults(cfg, false); d.Err != nil {
		t.Errorf("required with explicit store: err = %v, want nil", d.Err)
	}
}

func TestResolveAuthDefaults_RequiredAndDisabled_Errors(t *testing.T) {
	var cfg config
	WithAuthRequired()(&cfg)
	WithAuthDisabled()(&cfg)
	for _, hasDB := range []bool{true, false} {
		d := resolveAuthDefaults(cfg, hasDB)
		if d.Err == nil {
			t.Fatalf("hasDB=%v: got %+v, want an error", hasDB, d)
		}
		if got, want := d.Err.Error(), "adminapi: WithAuthRequired conflicts with WithAuthDisabled"; got != want {
			t.Errorf("hasDB=%v: err = %q, want %q", hasDB, got, want)
		}
	}
}

func TestResolveAuthDefaults_WithoutDefaultLogin(t *testing.T) {
	var cfg config
	WithoutDefaultLogin()(&cfg)
	d := resolveAuthDefaults(cfg, true)
	if !d.ProvisionKeyStore {
		t.Errorf("got %+v, want the KeyStore still provisioned", d)
	}
	if d.DefaultLogin {
		t.Errorf("got %+v, want no default login", d)
	}

	WithAuthRequired()(&cfg)
	if d := resolveAuthDefaults(cfg, true); d.Err != nil || !d.ProvisionKeyStore || d.DefaultLogin {
		t.Errorf("required + no default login: got %+v, want provision, no login, no error", d)
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
