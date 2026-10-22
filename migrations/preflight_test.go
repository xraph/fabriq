package migrations

import (
	"errors"
	"strings"
	"testing"
)

func TestPreloadedLibraries(t *testing.T) {
	tests := []struct {
		setting string
		want    string
		has     bool
	}{
		{"timescaledb", "timescaledb", true},
		{"pg_stat_statements, timescaledb", "timescaledb", true},
		{`"timescaledb",pgaudit`, "timescaledb", true},
		{"$libdir/timescaledb.so", "timescaledb", true},
		{"pg_stat_statements", "timescaledb", false},
		{"", "timescaledb", false},
	}
	for _, tc := range tests {
		if got := preloadedLibraries(tc.setting)[tc.want]; got != tc.has {
			t.Errorf("preloadedLibraries(%q)[%q] = %v, want %v", tc.setting, tc.want, got, tc.has)
		}
	}
}

func TestRedactSecrets(t *testing.T) {
	cause := errors.New("connection refused")

	t.Run("keeps the chain when nothing leaked", func(t *testing.T) {
		wrapped := errors.Join(errors.New("dial failed"), cause)
		got := redactSecrets(wrapped, "postgres://u:p@h/db", "hunter2")
		if !errors.Is(got, wrapped) || !errors.Is(got, cause) {
			t.Fatalf("redactSecrets replaced a clean error: %v", got)
		}
	})

	t.Run("scrubs the password and the dsn", func(t *testing.T) {
		leaky := errors.New("bad statement PASSWORD 'hunter2' via postgres://u:p@h/db")
		got := redactSecrets(leaky, "postgres://u:p@h/db", "hunter2")
		if strings.Contains(got.Error(), "hunter2") || strings.Contains(got.Error(), "postgres://") {
			t.Fatalf("secret survived: %v", got)
		}
	})

	t.Run("nil stays nil", func(t *testing.T) {
		if redactSecrets(nil, "d", "p") != nil {
			t.Fatal("nil error became non-nil")
		}
	})
}
