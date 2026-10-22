package main

import (
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/registry"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestEnvBool(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		def     bool
		want    bool
		wantErr bool
	}{
		{"empty uses default true", "", true, true, false},
		{"empty uses default false", "", false, false, false},
		{"true", "true", false, true, false},
		{"one", "1", false, true, false},
		{"false", "false", true, false, false},
		{"zero", "0", true, false, false},
		{"maybe", "maybe", true, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := envBool(envFrom(map[string]string{"FABRIQ_X": tc.val}), "FABRIQ_X", tc.def)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "FABRIQ_X") {
					t.Fatalf("err = %v, want an error naming FABRIQ_X", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEnvDuration(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		def     time.Duration
		want    time.Duration
		wantErr bool
	}{
		{"empty uses default", "", 15 * time.Second, 15 * time.Second, false},
		{"30s", "30s", time.Second, 30 * time.Second, false},
		{"zero", "0", time.Second, 0, false},
		{"soon", "soon", time.Second, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := envDuration(envFrom(map[string]string{"FABRIQ_D": tc.val}), "FABRIQ_D", tc.def)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "FABRIQ_D") {
					t.Fatalf("err = %v, want an error naming FABRIQ_D", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRegisterDomainPack(t *testing.T) {
	t.Run("example registers site", func(t *testing.T) {
		reg := registry.New()
		if err := registerDomainPack(reg, "example"); err != nil {
			t.Fatal(err)
		}
		if _, ok := reg.Get("site"); !ok {
			t.Fatal("example pack should register site")
		}
	})
	t.Run("empty means example", func(t *testing.T) {
		reg := registry.New()
		if err := registerDomainPack(reg, ""); err != nil {
			t.Fatal(err)
		}
		if _, ok := reg.Get("site"); !ok {
			t.Fatal("empty pack should register the example pack")
		}
	})
	t.Run("none registers nothing", func(t *testing.T) {
		reg := registry.New()
		if err := registerDomainPack(reg, "none"); err != nil {
			t.Fatal(err)
		}
		if n := len(reg.All()); n != 0 {
			t.Fatalf("none registered %d entities, want 0", n)
		}
	})
	t.Run("unknown errors", func(t *testing.T) {
		err := registerDomainPack(registry.New(), "x")
		if err == nil || err.Error() != `unknown FABRIQ_DOMAIN_PACK "x" (want example or none)` {
			t.Fatalf("err = %v", err)
		}
	})
}
