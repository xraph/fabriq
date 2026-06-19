package agentmcp

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/query"
)

func newCancelCtx() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// readSSEEvent reads lines from r until a blank-line-terminated SSE event or
// timeout. Returns the accumulated text of the event block.
func readSSEEvent(t *testing.T, r io.Reader, timeout time.Duration) string {
	t.Helper()
	type result struct {
		text string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		sc := bufio.NewScanner(r)
		var lines []string
		for sc.Scan() {
			line := sc.Text()
			lines = append(lines, line)
			// Blank line terminates an SSE event block.
			if line == "" && len(lines) > 1 {
				ch <- result{text: strings.Join(lines, "\n")}
				return
			}
		}
		if err := sc.Err(); err != nil {
			ch <- result{err: err}
		} else {
			ch <- result{text: strings.Join(lines, "\n")}
		}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("readSSEEvent: %v", res.err)
		}
		return res.text
	case <-time.After(timeout):
		t.Fatalf("readSSEEvent: timed out after %v", timeout)
		return ""
	}
}

func TestMCPWatch_SSEOverForgeRouter(t *testing.T) {
	tk, ff, _ := newToolkit(t) // package helper; ff is the pushable fake
	app := forge.NewApp(forge.AppConfig{Name: "mcp-watch-test", HTTPAddress: ":0"})
	e := &Extension{cfg: config{BasePath: "/api/v1/agent/mcp", WatchPath: "/api/v1/agent/mcp/watch"}, tk: tk}
	if err := app.RegisterController(newWatchController(e)); err != nil {
		t.Fatalf("register watch controller: %v", err)
	}
	srv := httptest.NewServer(app.Router().Handler())
	defer srv.Close()

	ctx, cancel := newCancelCtx()
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/api/v1/agent/mcp/watch",
		strings.NewReader(`{"entity":"mcpdoc","scope":"tenant"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST watch: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	// push a delta; it should arrive as an SSE event
	ff.pushDelta(query.Delta{StreamID: "e1", Aggregate: "mcpdoc", AggID: "d1", Type: "mcpdoc.created"})

	got := readSSEEvent(t, resp.Body, 2*time.Second)
	if !strings.Contains(got, "mcpdoc.created") || !strings.Contains(got, "d1") {
		t.Fatalf("SSE event missing delta: %q", got)
	}
}
