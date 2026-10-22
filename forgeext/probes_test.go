package forgeext

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xraph/forge"
)

// probeGet mounts the probes on a bare forge app and drives one GET.
func probeGet(t *testing.T, path string, checks ...ReadyCheck) (*httptest.ResponseRecorder, map[string]string) {
	t.Helper()
	app := forge.NewApp(forge.AppConfig{Name: "probes-test", HTTPAddress: ":0"})
	if err := MountProbes(app.Router(), checks...); err != nil {
		t.Fatalf("MountProbes: %v", err)
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	app.Router().ServeHTTP(w, req)
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q is not a JSON object: %v", w.Body.String(), err)
	}
	return w, body
}

func TestMountProbes_Livez200(t *testing.T) {
	// A failing check must not affect liveness.
	w, body := probeGet(t, "/_/livez", func(context.Context) error { return errors.New("down") })
	if w.Code != http.StatusOK {
		t.Fatalf("livez = %d, want 200", w.Code)
	}
	if body["status"] != "alive" {
		t.Fatalf("livez body = %v, want status alive", body)
	}
}

func TestMountProbes_ReadyzAllPass200(t *testing.T) {
	var order []string
	a := func(context.Context) error { order = append(order, "a"); return nil }
	b := func(context.Context) error { order = append(order, "b"); return nil }
	w, body := probeGet(t, "/_/readyz", a, b)
	if w.Code != http.StatusOK {
		t.Fatalf("readyz = %d, want 200; body %s", w.Code, w.Body.String())
	}
	if body["status"] != "ready" {
		t.Fatalf("readyz body = %v, want status ready", body)
	}
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("checks ran %v, want [a b]", order)
	}
}

func TestMountProbes_ReadyzNoChecks200(t *testing.T) {
	w, _ := probeGet(t, "/_/readyz")
	if w.Code != http.StatusOK {
		t.Fatalf("readyz with no checks = %d, want 200", w.Code)
	}
}

func TestMountProbes_ReadyzFailing503WithReason(t *testing.T) {
	ran := false
	ok := func(context.Context) error { return nil }
	bad := func(context.Context) error { return errors.New("postgres is down") }
	after := func(context.Context) error { ran = true; return errors.New("second failure") }
	w, body := probeGet(t, "/_/readyz", ok, bad, after)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d, want 503", w.Code)
	}
	if body["status"] != "not ready" {
		t.Fatalf("status = %q, want %q", body["status"], "not ready")
	}
	if body["reason"] != "postgres is down" {
		t.Fatalf("reason = %q, want the first failing check's message", body["reason"])
	}
	if ran {
		t.Fatal("checks after the first failure must not run")
	}
}

func TestMountProbes_ReadyzTimeout503(t *testing.T) {
	// The check ignores its context entirely, so only the handler's own
	// bound can save the probe.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	stuck := func(context.Context) error {
		<-release
		return nil
	}
	start := time.Now()
	w, body := probeGet(t, "/_/readyz", stuck)
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("readyz took %v, want under 3s", took)
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d, want 503", w.Code)
	}
	if body["reason"] == "" {
		t.Fatalf("reason is empty: %v", body)
	}
}

func TestExtensionReady_BeforeStart_Errors(t *testing.T) {
	e := New(nil)
	err := e.Ready(context.Background())
	if err == nil || err.Error() != "fabriq: not started" {
		t.Fatalf("Ready before Start = %v, want %q", err, "fabriq: not started")
	}
}
