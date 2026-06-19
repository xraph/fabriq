package agentmcp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xraph/forge"
)

// fakeBackedExt builds an Extension whose toolkit is pre-resolved (bypassing
// Start / fabriq.Open), mirroring gateway_http_test.go's fakeBackedGateway.
func fakeBackedExt(t testing.TB) *Extension {
	t.Helper()
	// reuse the dispatch_test toolkit builder (same package)
	tk, _, _ := newToolkit(t)
	return &Extension{cfg: config{BasePath: "/api/v1/agent/mcp"}, tk: tk}
}

func TestMCP_OverForgeRouter(t *testing.T) {
	app := forge.NewApp(forge.AppConfig{Name: "mcp-test", HTTPAddress: ":0"})
	e := fakeBackedExt(t)
	if err := app.RegisterController(newMCPController(e)); err != nil {
		t.Fatalf("register controller: %v", err)
	}
	srv := httptest.NewServer(app.Router().Handler())
	defer srv.Close()

	body := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`
	resp, err := http.Post(srv.URL+"/api/v1/agent/mcp", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), `"recall"`) {
		t.Fatalf("tools/list response missing recall: %s", buf[:n])
	}
}
