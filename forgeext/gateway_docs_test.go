package forgeext

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
)

// stubDocFacade backs the docs controller with the in-memory document
// store and controllable frame channels.
type stubDocFacade struct {
	store    *fabriqtest.FakeDocumentStore
	frames   chan query.Delta
	presence chan query.Delta

	mu        sync.Mutex
	published []string // nodeIDs of published presence frames
}

func (s *stubDocFacade) Document() document.Store { return s.store }

func (s *stubDocFacade) SubscribeDocument(context.Context, string) (<-chan query.Delta, error) {
	return s.frames, nil
}

func (s *stubDocFacade) SubscribeDocumentPresence(context.Context, string) (<-chan query.Delta, error) {
	return s.presence, nil
}

func (s *stubDocFacade) PublishDocumentPresence(_ context.Context, _, nodeID string, _ json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published = append(s.published, nodeID)
	return nil
}

func newDocsTestServer(t *testing.T) (*httptest.Server, *stubDocFacade) {
	t.Helper()
	app := forge.NewApp(forge.AppConfig{Name: "gw-docs-test", HTTPAddress: ":0"})
	facade := &stubDocFacade{
		store:    &fabriqtest.FakeDocumentStore{},
		frames:   make(chan query.Delta, 4),
		presence: make(chan query.Delta, 4),
	}
	g := &GatewayExtension{cfg: GatewayConfig{BasePath: "/api/v1/live"}}
	ctrl := newDocsController(g)
	ctrl.facade = facade
	if err := app.RegisterController(ctrl); err != nil {
		t.Fatalf("register docs controller: %v", err)
	}
	srv := httptest.NewServer(app.Router().Handler())
	t.Cleanup(srv.Close)
	return srv, facade
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestGatewayDocs_UpdateAndSync(t *testing.T) {
	srv, facade := newDocsTestServer(t)

	update := []byte(`[{"field":"title","crdt_type":"lww","hlc":{"ts":1,"c":0,"node":"a"},"node_id":"a","value":"hello"}]`)
	resp := postJSON(t, srv.URL+"/api/v1/live/docs/update", map[string]any{
		"docId":  "note/1",
		"update": base64.StdEncoding.EncodeToString(update),
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("update status = %d", resp.StatusCode)
	}

	// The update landed in the store.
	raw, err := facade.store.Sync(context.Background(), "note/1", nil)
	if err != nil || !strings.Contains(string(raw), "hello") {
		t.Fatalf("store sync = %s (%v)", raw, err)
	}

	// And the sync endpoint returns the same payload shape.
	resp2 := postJSON(t, srv.URL+"/api/v1/live/docs/sync", map[string]any{"docId": "note/1"})
	defer resp2.Body.Close()
	var payload struct {
		Seq     int64             `json:"seq"`
		Updates []json.RawMessage `json:"updates"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload.Seq != 1 || len(payload.Updates) != 1 {
		t.Fatalf("sync payload: %+v", payload)
	}
}

func TestGatewayDocs_UpdateValidation(t *testing.T) {
	srv, _ := newDocsTestServer(t)
	resp := postJSON(t, srv.URL+"/api/v1/live/docs/update", map[string]any{"docId": "note/1"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing update must 400, got %d", resp.StatusCode)
	}
}

func TestGatewayDocs_PresencePublish(t *testing.T) {
	srv, facade := newDocsTestServer(t)
	resp := postJSON(t, srv.URL+"/api/v1/live/docs/presence", map[string]any{
		"docId": "note/1", "node": "peer-1", "data": map[string]any{"cursor": 4},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("presence status = %d", resp.StatusCode)
	}
	facade.mu.Lock()
	defer facade.mu.Unlock()
	if len(facade.published) != 1 || facade.published[0] != "peer-1" {
		t.Fatalf("published = %v", facade.published)
	}
}

func TestGatewayDocs_SubscribeStreamsSyncAndPresence(t *testing.T) {
	srv, facade := newDocsTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		srv.URL+"/api/v1/live/docs/subscribe?id=note/1&presence=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer resp.Body.Close()

	facade.frames <- query.Delta{StreamID: "7", Type: "note.sync"}
	facade.presence <- query.Delta{StreamID: "p1", Type: "note.presence"}

	events := make(map[string]bool)
	scanner := bufio.NewScanner(resp.Body)
	deadline := time.After(4 * time.Second)
	for len(events) < 2 {
		lineCh := make(chan string, 1)
		go func() {
			if scanner.Scan() {
				lineCh <- scanner.Text()
			} else {
				lineCh <- fmt.Sprintf("<eof: %v>", scanner.Err())
			}
		}()
		select {
		case line := <-lineCh:
			if strings.HasPrefix(line, "event: ") {
				events[strings.TrimPrefix(line, "event: ")] = true
			}
		case <-deadline:
			t.Fatalf("timed out; saw events %v", events)
		}
	}
	if !events["sync"] || !events["presence"] {
		t.Fatalf("events = %v", events)
	}
}
