package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq/migrations"
)

// runMigrate executes `fabriq migrate <args...>` through a real forge CLI so
// flag parsing and the error-to-exit-code path are the production ones. It
// returns the exit code and captured stdout.
func runMigrate(t *testing.T, args ...string) (int, string) {
	t.Helper()
	var out bytes.Buffer
	c := cli.New(cli.Config{
		Name:         "fabriq",
		Output:       &out,
		ErrorHandler: func(error) {},
	})
	if err := c.AddCommand(migrateCommand()); err != nil {
		t.Fatal(err)
	}
	err := c.Run(append([]string{"fabriq", "migrate"}, args...))
	return cli.GetExitCode(err), out.String()
}

func fakeExtensions(t *testing.T, rep migrations.ExtensionReport, err error) *[]string {
	t.Helper()
	var gotRequired []string
	orig := checkExtensions
	checkExtensions = func(_ context.Context, _ string, required []string) (migrations.ExtensionReport, error) {
		gotRequired = required
		return rep, err
	}
	t.Cleanup(func() { checkExtensions = orig })
	return &gotRequired
}

func TestMigratePreflight_ExitCodes(t *testing.T) {
	okRep := migrations.ExtensionReport{
		OK: true,
		Extensions: []migrations.ExtensionStatus{
			{Name: "timescaledb", Available: true, Installed: "2.17.2", Preloaded: true, OK: true},
			{Name: "vector", Available: true, Preloaded: true, OK: true},
		},
	}
	badRep := migrations.ExtensionReport{
		OK: false,
		Extensions: []migrations.ExtensionStatus{
			{Name: "timescaledb", Available: true, Preloaded: false, OK: false},
		},
		Problems: []string{"timescaledb: not in shared_preload_libraries"},
	}

	tests := []struct {
		name     string
		args     []string
		rep      migrations.ExtensionReport
		err      error
		wantCode int
	}{
		{"ok", []string{"preflight", "--dsn", "postgres://x"}, okRep, nil, 0},
		{"failed report", []string{"preflight", "--dsn", "postgres://x"}, badRep, nil, 4},
		{"db error", []string{"preflight", "--dsn", "postgres://x"}, migrations.ExtensionReport{}, errors.New("boom"), 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeExtensions(t, tc.rep, tc.err)
			code, _ := runMigrate(t, tc.args...)
			if code != tc.wantCode {
				t.Fatalf("exit code = %d, want %d", code, tc.wantCode)
			}
		})
	}

	t.Run("missing dsn", func(t *testing.T) {
		t.Setenv("FABRIQ_POSTGRES_DSN", "")
		fakeExtensions(t, okRep, nil)
		if code, _ := runMigrate(t, "preflight"); code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
	})

	t.Run("dsn from env", func(t *testing.T) {
		t.Setenv("FABRIQ_POSTGRES_DSN", "postgres://x")
		fakeExtensions(t, okRep, nil)
		if code, _ := runMigrate(t, "preflight"); code != 0 {
			t.Fatalf("exit code = %d, want 0", code)
		}
	})

	t.Run("default and explicit require", func(t *testing.T) {
		got := fakeExtensions(t, okRep, nil)
		runMigrate(t, "preflight", "--dsn", "postgres://x")
		if strings.Join(*got, ",") != "timescaledb,vector,postgis" {
			t.Fatalf("default required = %v", *got)
		}
		runMigrate(t, "preflight", "--dsn", "postgres://x", "--require", "vector, postgis,")
		if strings.Join(*got, ",") != "vector,postgis" {
			t.Fatalf("explicit required = %v", *got)
		}
	})

	t.Run("json output", func(t *testing.T) {
		fakeExtensions(t, badRep, nil)
		code, out := runMigrate(t, "preflight", "--dsn", "postgres://x", "--json")
		if code != 4 {
			t.Fatalf("exit code = %d, want 4", code)
		}
		var rep migrations.ExtensionReport
		if err := json.Unmarshal([]byte(out), &rep); err != nil {
			t.Fatalf("stdout is not a single JSON document: %v\n%s", err, out)
		}
		if rep.OK || len(rep.Problems) != 1 {
			t.Fatalf("report = %+v", rep)
		}
	})

	t.Run("text output", func(t *testing.T) {
		fakeExtensions(t, badRep, nil)
		_, out := runMigrate(t, "preflight", "--dsn", "postgres://x")
		if !strings.Contains(out, "timescaledb") || !strings.Contains(out, "not in shared_preload_libraries") {
			t.Fatalf("text output missing detail:\n%s", out)
		}
	})
}

func TestMigrateAppRole_ExitCodes(t *testing.T) {
	type call struct{ dsn, role, password string }
	var calls []call
	stub := func(err error) {
		orig := ensureAppRole
		ensureAppRole = func(_ context.Context, dsn, role, password string) error {
			calls = append(calls, call{dsn, role, password})
			return err
		}
		t.Cleanup(func() { ensureAppRole = orig })
	}

	t.Run("ok", func(t *testing.T) {
		calls = nil
		stub(nil)
		t.Setenv("FABRIQ_APP_ROLE_PASSWORD", "s3cret-pw")
		code, out := runMigrate(t, "app-role", "--dsn", "postgres://x", "--role", "fabriq_app")
		if code != 0 {
			t.Fatalf("exit code = %d, want 0", code)
		}
		if len(calls) != 1 || calls[0] != (call{"postgres://x", "fabriq_app", "s3cret-pw"}) {
			t.Fatalf("calls = %+v", calls)
		}
		if strings.Contains(out, "s3cret-pw") {
			t.Fatalf("password leaked to output: %s", out)
		}
	})

	t.Run("db error", func(t *testing.T) {
		calls = nil
		stub(errors.New("boom"))
		t.Setenv("FABRIQ_APP_ROLE_PASSWORD", "pw")
		if code, _ := runMigrate(t, "app-role", "--dsn", "postgres://x", "--role", "fabriq_app"); code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
	})

	t.Run("refused by the library is a usage error", func(t *testing.T) {
		calls = nil
		stub(fmt.Errorf("%w: role is the connecting role", migrations.ErrInvalidAppRole))
		t.Setenv("FABRIQ_APP_ROLE_PASSWORD", "pw")
		if code, _ := runMigrate(t, "app-role", "--dsn", "postgres://x", "--role", "fabriq"); code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
	})

	t.Run("missing password", func(t *testing.T) {
		calls = nil
		stub(nil)
		t.Setenv("FABRIQ_APP_ROLE_PASSWORD", "")
		if code, _ := runMigrate(t, "app-role", "--dsn", "postgres://x", "--role", "fabriq_app"); code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
		if len(calls) != 0 {
			t.Fatalf("EnsureAppRole called without a password")
		}
	})

	t.Run("missing role", func(t *testing.T) {
		calls = nil
		stub(nil)
		t.Setenv("FABRIQ_APP_ROLE_PASSWORD", "pw")
		if code, _ := runMigrate(t, "app-role", "--dsn", "postgres://x"); code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
		if len(calls) != 0 {
			t.Fatalf("EnsureAppRole called without a role")
		}
	})

	t.Run("invalid role", func(t *testing.T) {
		calls = nil
		stub(nil)
		t.Setenv("FABRIQ_APP_ROLE_PASSWORD", "pw")
		if code, _ := runMigrate(t, "app-role", "--dsn", "postgres://x", "--role", "Bad-Role"); code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
		if len(calls) != 0 {
			t.Fatalf("EnsureAppRole called with an invalid role")
		}
	})
}
