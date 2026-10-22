package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/xraph/forge/cli"
)

// runRebuild executes `fabriq rebuild <args...>` through a real forge CLI, so
// flag parsing and the error-to-exit-code path are the production ones. It
// returns the exit code, the captured output and the error.
func runRebuild(t *testing.T, args ...string) (int, string, error) {
	t.Helper()
	var out bytes.Buffer
	c := cli.New(cli.Config{
		Name:         "fabriq",
		Output:       &out,
		ErrorHandler: func(error) {},
	})
	if err := c.AddCommand(rebuildCommand()); err != nil {
		t.Fatal(err)
	}
	err := c.Run(append([]string{"fabriq", "rebuild"}, args...))
	return cli.GetExitCode(err), out.String(), err
}

// clearStoreEnv blanks every variable the operator commands read, so the
// developer's own environment cannot leak into a case.
func clearStoreEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"FABRIQ_POSTGRES_DSN",
		"FABRIQ_REDIS_ADDR", "FABRIQ_REDIS_USERNAME", "FABRIQ_REDIS_PASSWORD",
		"FABRIQ_FALKORDB_ADDR", "FABRIQ_FALKORDB_USERNAME", "FABRIQ_FALKORDB_PASSWORD",
		"FABRIQ_ELASTICSEARCH_ADDRS", "FABRIQ_ELASTICSEARCH_USERNAME", "FABRIQ_ELASTICSEARCH_PASSWORD",
		"FABRIQ_DOMAIN_PACK", "FABRIQ_DYNAMIC_PERSIST",
	} {
		t.Setenv(k, "")
	}
}

// TestRebuild_UsageExitCodes pins exit code 2 for every usage error. Each case
// fails before anything is dialled, so the DSN is never used.
func TestRebuild_UsageExitCodes(t *testing.T) {
	const dsn = "postgres://unused/fabriq"
	tests := []struct {
		name    string
		env     map[string]string
		args    []string
		wantMsg string
	}{
		{"bad projection", nil,
			[]string{"--dsn", dsn, "--projection", "vector", "--tenant", "acme", "--falkordb", "f:6379"},
			"--projection must be graph or search"},
		{"no tenant selector", nil,
			[]string{"--dsn", dsn, "--projection", "graph", "--falkordb", "f:6379"},
			"--tenant is required"},
		{"graph without falkordb", nil,
			[]string{"--dsn", dsn, "--projection", "graph", "--tenant", "acme"},
			"--falkordb (or FABRIQ_FALKORDB_ADDR) is required"},
		{"search without elasticsearch", nil,
			[]string{"--dsn", dsn, "--projection", "search", "--all-tenants"},
			"--elasticsearch (or FABRIQ_ELASTICSEARCH_ADDRS) is required"},
		{"unknown domain pack", map[string]string{"FABRIQ_DOMAIN_PACK": "x"},
			[]string{"--dsn", dsn, "--projection", "graph", "--tenant", "acme", "--falkordb", "f:6379"},
			"unknown FABRIQ_DOMAIN_PACK"},
		{"bad dynamic persist", map[string]string{"FABRIQ_DYNAMIC_PERSIST": "maybe"},
			[]string{"--dsn", dsn, "--projection", "graph", "--tenant", "acme", "--falkordb", "f:6379"},
			"FABRIQ_DYNAMIC_PERSIST"},
		{"finalize without tenant", nil,
			[]string{"finalize", "--dsn", dsn, "--falkordb", "f:6379"},
			"--tenant and --falkordb (or FABRIQ_FALKORDB_ADDR) are required"},
		{"finalize without falkordb", nil,
			[]string{"finalize", "--dsn", dsn, "--tenant", "acme"},
			"--tenant and --falkordb (or FABRIQ_FALKORDB_ADDR) are required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearStoreEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			code, _, err := runRebuild(t, tc.args...)
			if code != exitUsage {
				t.Fatalf("exit code = %d (err %v), want %d", code, err, exitUsage)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.wantMsg)
			}
		})
	}
}
