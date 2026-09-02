package adminapi

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/subscribe"
)

// postLive issues a POST to {srv}/admin/live with the test tenant header and
// the given JSON body.
func postLive(t *testing.T, srv *httptest.Server, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/live", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(testTenantHeader, testTenantID)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// TestAdminLive_NotConfigured verifies that the fake-backed extension (no
// concrete *fabriq.Fabriq facade) degrades the live endpoint to 501 with the
// documented not-configured contract.
func TestAdminLive_NotConfigured(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postLive(t, srv, `{"entity":"widget"}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 501; body = %s", resp.StatusCode, body)
	}

	var got map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got["error"] != "live queries not configured" {
		t.Errorf("error = %q, want %q", got["error"], "live queries not configured")
	}
}

// TestAdminLive_Capability verifies "live.read" is advertised in the static
// capability set served by GET /admin/meta.
func TestAdminLive_Capability(t *testing.T) {
	found := false
	for _, cap := range capabilities {
		if cap == "live.read" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("capabilities missing %q: %v", "live.read", capabilities)
	}
}

// TestAdminLive_MissingEntity verifies that a body without "entity" is rejected
// with 400 — but only when the live facade is available. The fake-backed
// extension short-circuits to 501 before reading the body, so this asserts the
// validation order: the not-configured guard runs first (the body is only
// validated once a concrete facade exists). We therefore assert that an empty
// body still surfaces the not-configured 501 (never a 500/panic).
func TestAdminLive_EmptyBody(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postLive(t, srv, ``)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 501; body = %s", resp.StatusCode, body)
	}
}

// TestWriteLiveSnapshot_Framing verifies the snapshot SSE event framing: the
// "snapshot" event name, the {type:"snapshot", rows:[...]} payload, the row id
// + raw row mapping, and the limit cap.
func TestWriteLiveSnapshot_Framing(t *testing.T) {
	rec := httptest.NewRecorder()
	sse, err := subscribe.NewSSEWriter(rec)
	if err != nil {
		t.Fatalf("new sse writer: %v", err)
	}

	snap := livequery.Snapshot{
		Watermark: "wm-1",
		Rows: []livequery.Row{
			{AggID: "a", Raw: json.RawMessage(`{"id":"a","name":"Alpha"}`)},
			{AggID: "b", Raw: json.RawMessage(`{"id":"b","name":"Bravo"}`)},
			{AggID: "c", Raw: json.RawMessage(`{"id":"c","name":"Cara"}`)},
		},
	}

	// Cap at 2 to exercise the limit truncation.
	if err := writeLiveSnapshot(sse, snap, 2); err != nil {
		t.Fatalf("writeLiveSnapshot: %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "event: snapshot") {
		t.Fatalf("missing snapshot event name; body = %q", body)
	}
	if !strings.Contains(body, "id: wm-1") {
		t.Errorf("missing watermark as SSE id; body = %q", body)
	}

	ev := parseFirstSSEData(t, body)
	var got liveSnapshotEvent
	if err := json.Unmarshal([]byte(ev), &got); err != nil {
		t.Fatalf("unmarshal snapshot event %q: %v", ev, err)
	}
	if got.Type != "snapshot" {
		t.Errorf("type = %q, want %q", got.Type, "snapshot")
	}
	if len(got.Rows) != 2 {
		t.Fatalf("rows len = %d, want 2 (limit cap)", len(got.Rows))
	}
	if got.Rows[0].ID != "a" || got.Rows[1].ID != "b" {
		t.Errorf("row ids = [%q %q], want [a b]", got.Rows[0].ID, got.Rows[1].ID)
	}
	// The raw row payload is forwarded verbatim.
	var first map[string]any
	if err := json.Unmarshal(got.Rows[0].Row, &first); err != nil {
		t.Fatalf("unmarshal row[0].row: %v", err)
	}
	if first["name"] != "Alpha" {
		t.Errorf("row[0] name = %v, want Alpha", first["name"])
	}
}

// parseFirstSSEData extracts the first "data:" line payload from an SSE body.
func parseFirstSSEData(t *testing.T, body string) string {
	t.Helper()
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			return strings.TrimPrefix(line, "data: ")
		}
	}
	t.Fatalf("no data line in SSE body %q", body)
	return ""
}

/*
THE FIELD DELTA HAS TO SURVIVE THE DTO.

The SSE surface does not marshal a livequery.LiveDelta — it maps one onto
liveDeltaEvent — so a field the engine computes reaches no browser until this
type carries it. The engine's own tests prove the delta is produced; these
prove it is not dropped on the way out, which is a different failure and was
the state of this endpoint before.
*/
func TestLiveDeltaEvent_CarriesFieldChanges(t *testing.T) {
	tests := []struct {
		name    string
		delta   livequery.LiveDelta
		wantOp  string
		want    map[string]any
		wantKey bool // whether "changes" should appear on the wire at all
	}{
		{
			name: "an update names the fields that moved",
			delta: livequery.LiveDelta{
				Op: livequery.OpUpdate, AggID: "a", OldIndex: 1, NewIndex: 1,
				Row:     json.RawMessage(`{"id":"a","temp":11}`),
				Changes: map[string]any{"temp": 11.0},
			},
			wantOp: "update", want: map[string]any{"temp": 11.0}, wantKey: true,
		},
		{
			name: "a move carries them too",
			delta: livequery.LiveDelta{
				Op: livequery.OpMove, AggID: "a", OldIndex: 3, NewIndex: 0,
				Row:     json.RawMessage(`{"id":"a","name":"z"}`),
				Changes: map[string]any{"name": "z"},
			},
			wantOp: "move", want: map[string]any{"name": "z"}, wantKey: true,
		},
		{
			// omitempty, so a subscription that did not ask for field deltas
			// pays no bytes for a key it never reads.
			name: "no delta means no key on the wire",
			delta: livequery.LiveDelta{
				Op: livequery.OpUpdate, AggID: "a",
				Row: json.RawMessage(`{"id":"a"}`),
			},
			wantOp: "update", want: nil, wantKey: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// THE HANDLER'S OWN MAPPING, not a copy of it. Rebuilding the
			// struct here made this test pass no matter what the handler did:
			// both halves of the wiring could be deleted and it stayed green.
			body, err := json.Marshal(deltaEventFrom(tt.delta))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			var got map[string]any
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("unmarshal %s: %v", body, err)
			}
			if got["op"] != tt.wantOp {
				t.Errorf("op = %v, want %q", got["op"], tt.wantOp)
			}
			raw, present := got["changes"]
			if present != tt.wantKey {
				t.Fatalf("changes present = %v, want %v; body = %s", present, tt.wantKey, body)
			}
			if tt.wantKey {
				if !reflect.DeepEqual(raw, tt.want) {
					t.Errorf("changes = %#v, want %#v", raw, tt.want)
				}
			}
			// ADDITIVE: the whole row still ships either way.
			if _, ok := got["row"]; !ok {
				t.Error("row must still be sent beside the delta")
			}
		})
	}
}

/*
GUARDS the request half. A client that cannot ASK for field deltas cannot have
them, however well the engine computes them.
*/
func TestLiveRequest_CarriesFieldDeltasFlag(t *testing.T) {
	var req liveRequest
	if err := json.Unmarshal([]byte(`{"entity":"asset","fieldDeltas":true}`), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !req.FieldDeltas {
		t.Fatal("fieldDeltas did not decode off the request body")
	}
	// AND REACHES THE ENGINE. Decoding it onto the request proves nothing on
	// its own: the flag has to arrive on the query the engine is handed.
	if !liveQueryFrom(req, 50).FieldDeltas {
		t.Error("the request asked for field deltas and the query did not carry it")
	}

	// And stays off when not asked, which is what keeps the wire unchanged for
	// every client written before this existed.
	var plain liveRequest
	if err := json.Unmarshal([]byte(`{"entity":"asset"}`), &plain); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if plain.FieldDeltas || liveQueryFrom(plain, 50).FieldDeltas {
		t.Error("fieldDeltas defaulted to true")
	}
}
