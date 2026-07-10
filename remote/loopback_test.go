package remote_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/remote"
)

// testAsset is a minimal static grove model (mirrors the example domain shape without
// importing domain/). The server decodes the opaque wire payload back into a
// fresh instance of this type via the registry.
type testAsset struct {
	grove.BaseModel `grove:"table:assets"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
}

func assetRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	r := registry.New()
	if err := r.Register(registry.EntitySpec{
		Name:  "asset",
		Kind:  registry.KindAggregate,
		Model: (*testAsset)(nil),
	}); err != nil {
		t.Fatalf("register asset: %v", err)
	}
	return r
}

// fakeFabric is the embedded facade the Handler delegates to. It records the
// command(s) it received (so the test can assert the server-side payload
// decode) and returns a programmed result/error. Embedding query.Fabric means
// the methods the write plane never calls stay unimplemented.
type fakeFabric struct {
	query.Fabric
	rel       *fakeRelational
	subCh     chan query.Delta
	subErr    error
	liveSnap  livequery.Snapshot
	liveCh    chan livequery.LiveDelta
	liveErr   error
	liveSub   *fakeLiveSub
	blobStore blob.Store
	retr      *fakeRetrieval
	ts        *fakeTS
	sp        *fakeSpatial
	doc       *fakeDocStore
	gotCmd    command.Command
	gotCmds   []command.Command
	result    command.Result
	err       error
}

func (f *fakeFabric) Relational() query.RelationalQuerier { return f.rel }

func (f *fakeFabric) Timeseries() query.TSQuerier { return f.ts }

func (f *fakeFabric) Spatial() query.SpatialQuerier { return f.sp }

func (f *fakeFabric) Document() document.Store { return f.doc }

func (f *fakeFabric) Subscribe(_ context.Context, _ query.SubscribeScope) (<-chan query.Delta, error) {
	if f.subErr != nil {
		return nil, f.subErr
	}
	return f.subCh, nil
}

// LiveQuery makes fakeFabric satisfy remote.LiveQuerier. It returns a
// fakeLiveSub as the control surface so tests can drive Reanchor and inspect the
// cursor the "engine" saw; when liveSub is nil the subscription is nil (the
// server tolerates that, mirroring a facade that returns no handle).
func (f *fakeFabric) LiveQuery(_ context.Context, _ livequery.LiveQuery) (livequery.Snapshot, <-chan livequery.LiveDelta, remote.LiveSubscription, error) {
	if f.liveErr != nil {
		return livequery.Snapshot{}, nil, nil, f.liveErr
	}
	if f.liveSub != nil {
		return f.liveSnap, f.liveCh, f.liveSub, nil
	}
	return f.liveSnap, f.liveCh, nil, nil
}

// fakeLiveSub is a test double for the in-process live subscription: it records
// the cursor/limit each Reanchor received and returns a programmed fresh
// snapshot, standing in for *livequery.Handle over the bidi wire.
type fakeLiveSub struct {
	mu          sync.Mutex
	reanchSnap  livequery.Snapshot
	reanchErr   error
	gotCursor   *livequery.Cursor
	gotLimit    int
	reanchorHit int
	closed      bool
}

func (s *fakeLiveSub) Reanchor(_ context.Context, cursor *livequery.Cursor, limit int) (livequery.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gotCursor = cursor
	s.gotLimit = limit
	s.reanchorHit++
	if s.reanchErr != nil {
		return livequery.Snapshot{}, s.reanchErr
	}
	return s.reanchSnap, nil
}

func (s *fakeLiveSub) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

func (f *fakeFabric) Blob() blob.Store            { return f.blobStore }
func (f *fakeFabric) Vector() query.VectorQuerier { return f.retr }
func (f *fakeFabric) Search() query.SearchQuerier { return f.retr }
func (f *fakeFabric) Graph() query.GraphQuerier   { return f.retr }

// fakeRetrieval implements the three projection-read ports recall fuses
// (VectorQuerier + SearchQuerier + GraphQuerier — their method sets don't
// collide), recording the queries it receives and returning canned results.
type fakeRetrieval struct {
	matches    []query.VectorMatch
	hits       []map[string]any
	ids        []string
	rows       []map[string]any
	gotVecQ    query.VectorQuery
	gotSearchQ query.SearchQuery
	gotCypher  string
	// Programmed responses for Vector().Get; when vecStore is non-nil the Get
	// method looks up the id in the map. An absent id returns ErrNotFound.
	vecStore map[string][]float32
	// gotDelByMetaEntity and gotDelByMetaFilter record the last DeleteByMeta call.
	gotDelByMetaEntity string
	gotDelByMetaFilter map[string]string
}

func (f *fakeRetrieval) Similar(_ context.Context, q query.VectorQuery, into any) error {
	f.gotVecQ = q
	if p, ok := into.(*[]query.VectorMatch); ok {
		*p = f.matches
	}
	return nil
}
func (f *fakeRetrieval) Upsert(_ context.Context, _ string, id string, vec []float32, _ map[string]any) error {
	if f.vecStore != nil {
		f.vecStore[id] = vec
	}
	return nil
}
func (f *fakeRetrieval) Delete(context.Context, string, string) error { return nil }
func (f *fakeRetrieval) DeleteByMeta(_ context.Context, entity string, filter map[string]string) error {
	f.gotDelByMetaEntity = entity
	f.gotDelByMetaFilter = filter
	return nil
}
func (f *fakeRetrieval) Get(_ context.Context, _ string, id string) ([]float32, error) {
	if f.vecStore == nil {
		return nil, nil
	}
	vec, ok := f.vecStore[id]
	if !ok {
		return nil, fabriqerr.ErrNotFound
	}
	return vec, nil
}

func (f *fakeRetrieval) Search(_ context.Context, q query.SearchQuery, into any) error {
	f.gotSearchQ = q
	if p, ok := into.(*[]map[string]any); ok {
		*p = f.hits
	}
	return nil
}
func (f *fakeRetrieval) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return nil
}

func (f *fakeRetrieval) Query(_ context.Context, cypher string, _ map[string]any, into any) error {
	f.gotCypher = cypher
	switch p := into.(type) {
	case *[]string:
		*p = f.ids
	case *[]map[string]any:
		*p = f.rows
	}
	return nil
}
func (f *fakeRetrieval) TraverseAndHydrate(context.Context, string, map[string]any, any) error {
	return nil
}

// fakeTS implements query.TSQuerier, recording what it received and returning
// programmed rows for Range.
type fakeTS struct {
	gotPoints []query.Point
	gotRange  query.RangeQuery
	rows      []map[string]any
}

func (f *fakeTS) BulkWrite(_ context.Context, _ string, pts []query.Point) error {
	f.gotPoints = pts
	return nil
}
func (f *fakeTS) Range(_ context.Context, q query.RangeQuery, into any) error {
	f.gotRange = q
	if p, ok := into.(*[]map[string]any); ok {
		*p = f.rows
	}
	return nil
}

// fakeSpatial implements query.SpatialQuerier, recording what it received and
// returning a canned Get result.
type fakeSpatial struct {
	gotGeom   query.Geometry
	gotMeta   map[string]any
	gotWithin query.SpatialQuery
	rows      []map[string]any
	getGeom   query.Geometry
	getMeta   map[string]any
	getOK     bool
}

func (f *fakeSpatial) Upsert(_ context.Context, _, _ string, geom query.Geometry, meta map[string]any) error {
	f.gotGeom = geom
	f.gotMeta = meta
	return nil
}
func (f *fakeSpatial) Within(_ context.Context, q query.SpatialQuery, into any) error {
	f.gotWithin = q
	if p, ok := into.(*[]map[string]any); ok {
		*p = f.rows
	}
	return nil
}
func (f *fakeSpatial) Get(_ context.Context, _, _ string) (query.Geometry, map[string]any, bool, error) {
	return f.getGeom, f.getMeta, f.getOK, nil
}
func (f *fakeSpatial) Delete(_ context.Context, _, _ string) error { return nil }

// fakeDocStore implements document.Store, recording the ApplyUpdate/Compact
// calls it received and returning canned Sync/Snapshot replies.
type fakeDocStore struct {
	gotApplyDocID  string
	gotApplyUpdate []byte
	gotSyncDocID   string
	gotSyncSV      []byte
	syncUpdate     []byte
	syncErr        error
	snapshot       document.Materialized
	snapshotErr    error
	gotCompactDoc  string
	compactErr     error
}

func (f *fakeDocStore) ApplyUpdate(_ context.Context, docID string, update []byte) error {
	f.gotApplyDocID = docID
	f.gotApplyUpdate = update
	return nil
}

func (f *fakeDocStore) Sync(_ context.Context, docID string, stateVector []byte) ([]byte, error) {
	f.gotSyncDocID = docID
	f.gotSyncSV = stateVector
	return f.syncUpdate, f.syncErr
}

func (f *fakeDocStore) Snapshot(_ context.Context, _ string) (document.Materialized, error) {
	return f.snapshot, f.snapshotErr
}

func (f *fakeDocStore) Compact(_ context.Context, docID string) error {
	f.gotCompactDoc = docID
	return f.compactErr
}

// fakeBlob is a minimal in-memory blob.Store (+ Presigner) for the byte-plane
// tests.
type fakeBlob struct {
	mu   sync.Mutex
	data map[string][]byte

	listResult []blob.ObjectInfo
	copyResult blob.ObjectInfo
}

func (b *fakeBlob) Put(_ context.Context, key string, r io.Reader, o blob.PutOpts) (blob.ObjectInfo, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return blob.ObjectInfo{}, err
	}
	b.mu.Lock()
	if b.data == nil {
		b.data = map[string][]byte{}
	}
	b.data[key] = body
	b.mu.Unlock()
	return blob.ObjectInfo{Key: key, Size: int64(len(body)), ContentType: o.ContentType, Checksum: "sum"}, nil
}

func (b *fakeBlob) Get(_ context.Context, key string) (io.ReadCloser, blob.ObjectInfo, error) {
	b.mu.Lock()
	body, ok := b.data[key]
	b.mu.Unlock()
	if !ok {
		return nil, blob.ObjectInfo{}, fabriqerr.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(body)), blob.ObjectInfo{Key: key, Size: int64(len(body))}, nil
}

func (b *fakeBlob) Head(_ context.Context, key string) (blob.ObjectInfo, error) {
	b.mu.Lock()
	body, ok := b.data[key]
	b.mu.Unlock()
	if !ok {
		return blob.ObjectInfo{}, fabriqerr.ErrNotFound
	}
	return blob.ObjectInfo{Key: key, Size: int64(len(body))}, nil
}

func (b *fakeBlob) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	delete(b.data, key)
	b.mu.Unlock()
	return nil
}

func (b *fakeBlob) List(context.Context, string) ([]blob.ObjectInfo, error) {
	return b.listResult, nil
}
func (b *fakeBlob) Copy(context.Context, string, string) (blob.ObjectInfo, error) {
	return b.copyResult, nil
}
func (b *fakeBlob) Capabilities() blob.Caps { return blob.Caps{Presign: true} }
func (b *fakeBlob) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	return "https://store/get/" + key, nil
}
func (b *fakeBlob) PresignPut(_ context.Context, key string, _ time.Duration) (string, error) {
	return "https://store/put/" + key, nil
}

// fakeRelational stands in for the relational port. Get/GetMany fill the
// caller's scan target — which the server builds from the registry, so it is
// *testAsset / *[]*testAsset here; a non-nil err short-circuits to a typed
// failure.
type fakeRelational struct {
	one     *testAsset
	many    []*testAsset
	err     error
	gotList query.ListQuery
}

func (f *fakeRelational) Get(_ context.Context, _, _ string, into any) error {
	if f.err != nil {
		return f.err
	}
	p, ok := into.(*testAsset)
	if !ok {
		return fmt.Errorf("fakeRelational.Get: unexpected into %T", into)
	}
	if f.one != nil {
		*p = *f.one
	}
	return nil
}

func (f *fakeRelational) GetMany(_ context.Context, _ string, _ []string, into any) error {
	if f.err != nil {
		return f.err
	}
	p, ok := into.(*[]*testAsset)
	if !ok {
		return fmt.Errorf("fakeRelational.GetMany: unexpected into %T", into)
	}
	*p = f.many
	return nil
}

func (f *fakeRelational) List(_ context.Context, _ string, q query.ListQuery, into any) error {
	f.gotList = q
	if f.err != nil {
		return f.err
	}
	p, ok := into.(*[]*testAsset)
	if !ok {
		return fmt.Errorf("fakeRelational.List: unexpected into %T", into)
	}
	*p = f.many
	return nil
}

func (f *fakeRelational) Query(context.Context, any, string, ...any) error {
	return errors.New("fakeRelational.Query: unused")
}

func (f *fakeFabric) Exec(_ context.Context, cmd command.Command) (command.Result, error) {
	f.gotCmd = cmd
	return f.result, f.err
}

func (f *fakeFabric) ExecBatch(_ context.Context, cmds []command.Command) ([]command.Result, error) {
	f.gotCmds = cmds
	if f.err != nil {
		return nil, f.err
	}
	out := make([]command.Result, len(cmds))
	for i := range cmds {
		out[i] = command.Result{AggID: fmt.Sprintf("agg-%d", i), Version: 1}
	}
	return out, nil
}

// wire builds a Fabric talking to a Handler over the in-process Loopback
// transport — the whole client→envelope→server→envelope→client round trip with
// no network.
func wire(t *testing.T, fab query.Fabric) *remote.Fabric {
	t.Helper()
	h := remote.NewHandler(fab, assetRegistry(t))
	return remote.New(remote.Loopback{Handler: h})
}

// TestLoopback_ExecRoundTrip proves a create survives the envelope both ways:
// the typed payload reaches the server decoded back into *testAsset, and the
// result reaches the client intact.
func TestLoopback_ExecRoundTrip(t *testing.T) {
	fake := &fakeFabric{result: command.Result{AggID: "asset-1", Version: 1, EventID: "evt-1"}}
	client := wire(t, fake)

	res, err := client.Exec(context.Background(), command.Command{
		Entity:  "asset",
		Op:      command.OpCreate,
		Payload: &testAsset{ID: "asset-1", TenantID: "acme", Name: "Pump A"},
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}

	// Result reconstructed client-side.
	if res.AggID != "asset-1" || res.Version != 1 || res.EventID != "evt-1" {
		t.Fatalf("result = %+v, want {asset-1 1 evt-1}", res)
	}

	// Server received the command with op + a registry-typed payload (the crux:
	// opaque JSON on the wire, *testAsset at the facade).
	if fake.gotCmd.Op != command.OpCreate {
		t.Errorf("server op = %v, want OpCreate", fake.gotCmd.Op)
	}
	got, ok := fake.gotCmd.Payload.(*testAsset)
	if !ok {
		t.Fatalf("server payload type = %T, want *testAsset", fake.gotCmd.Payload)
	}
	if got.ID != "asset-1" || got.Name != "Pump A" || got.TenantID != "acme" {
		t.Errorf("decoded payload = %+v, want {asset-1 acme Pump A}", got)
	}
}

// TestLoopback_VersionConflictSurvivesWire proves the typed-error taxonomy
// crosses the envelope: a VersionConflictError at the facade comes back as an
// errors.Is-matchable ErrVersionConflict at the client.
func TestLoopback_VersionConflictSurvivesWire(t *testing.T) {
	fake := &fakeFabric{err: &fabriqerr.VersionConflictError{
		Aggregate: "asset", AggID: "asset-1", Expected: 1, Actual: 2,
	}}
	client := wire(t, fake)

	_, err := client.Exec(context.Background(), command.Command{
		Entity:  "asset",
		Op:      command.OpUpdate,
		AggID:   "asset-1",
		Payload: &testAsset{ID: "asset-1", TenantID: "acme", Name: "Pump A"},
	})
	if err == nil {
		t.Fatal("Exec: want error, got nil")
	}
	if !errors.Is(err, fabriqerr.ErrVersionConflict) {
		t.Fatalf("error %v does not match ErrVersionConflict", err)
	}
}

// TestLoopback_ExecBatchRoundTrip proves N commands cross the envelope and N
// results come back, in order.
func TestLoopback_ExecBatchRoundTrip(t *testing.T) {
	fake := &fakeFabric{}
	client := wire(t, fake)

	results, err := client.ExecBatch(context.Background(), []command.Command{
		{Entity: "asset", Op: command.OpCreate, Payload: &testAsset{ID: "a0", Name: "A"}},
		{Entity: "asset", Op: command.OpCreate, Payload: &testAsset{ID: "a1", Name: "B"}},
	})
	if err != nil {
		t.Fatalf("ExecBatch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].AggID != "agg-0" || results[1].AggID != "agg-1" {
		t.Errorf("results = %+v, want agg-0, agg-1", results)
	}
	if len(fake.gotCmds) != 2 || fake.gotCmds[1].AggID != "" {
		t.Errorf("server received %d commands", len(fake.gotCmds))
	}
}

// TestLoopback_UnwiredPlanesError proves the not-yet-wired planes are a safe
// drop-in: they return ErrNotImplemented rather than panicking.
func TestLoopback_UnwiredPlanesError(t *testing.T) {
	client := wire(t, &fakeFabric{})

	if err := client.Relational().Query(context.Background(), nil, "SELECT 1"); !errors.Is(err, remote.ErrNotImplemented) {
		t.Errorf("Relational().Query err = %v, want ErrNotImplemented", err)
	}
	if err := client.Graph().TraverseAndHydrate(context.Background(), "MATCH (n) RETURN n", nil, nil); !errors.Is(err, remote.ErrNotImplemented) {
		t.Errorf("Graph().TraverseAndHydrate err = %v, want ErrNotImplemented", err)
	}
}

// TestLoopback_SubscribeStreamsDeltas proves the server-streaming seam: deltas
// pushed on the facade's channel arrive in order on the client's channel, and
// the client channel closes when the server stream ends.
func TestLoopback_SubscribeStreamsDeltas(t *testing.T) {
	ch := make(chan query.Delta, 2)
	ch <- query.Delta{AggID: "a", Type: "updated"}
	ch <- query.Delta{AggID: "b", Type: "created"}
	close(ch)
	client := wire(t, &fakeFabric{subCh: ch})

	out, err := client.Subscribe(context.Background(), query.SubscribeScope{Entity: "asset", Scope: "tenant"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	got := make([]string, 0, 2)
	for d := range out {
		got = append(got, d.AggID)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v, want [a b]", got)
	}
}

// TestLoopback_SubscribeAuthzErrorSurvivesWire proves the handshake frame
// carries a setup failure back synchronously as a typed sentinel.
func TestLoopback_SubscribeAuthzErrorSurvivesWire(t *testing.T) {
	client := wire(t, &fakeFabric{subErr: tenant.ErrNoTenant})

	_, err := client.Subscribe(context.Background(), query.SubscribeScope{Entity: "asset"})
	if !errors.Is(err, tenant.ErrNoTenant) {
		t.Fatalf("Subscribe err = %v, want ErrNoTenant", err)
	}
}

// TestLoopback_BidiStreamEchoInterleaves proves the bidirectional transport
// primitive: the client Sends frames and Recvs the handler's replies
// independently, interleaved on one open stream, and Recv reports io.EOF once
// the client stops sending and the handler returns.
func TestLoopback_BidiStreamEchoInterleaves(t *testing.T) {
	h := remote.NewHandler(&fakeFabric{}, assetRegistry(t))
	tr := remote.Loopback{Handler: h}

	conn, err := tr.BidiStream(context.Background(), remote.MethodBidiEcho)
	if err != nil {
		t.Fatalf("BidiStream: %v", err)
	}
	for _, msg := range []string{"one", "two", "three"} {
		if err := conn.Send([]byte(msg)); err != nil {
			t.Fatalf("Send %q: %v", msg, err)
		}
		got, rerr := conn.Recv()
		if rerr != nil {
			t.Fatalf("Recv after %q: %v", msg, rerr)
		}
		if string(got) != "echo:"+msg {
			t.Fatalf("Recv = %q, want %q", got, "echo:"+msg)
		}
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// After Close the stream is torn down; Recv reports the terminal error (a
	// cancellation or io.EOF), never another frame.
	if _, err := conn.Recv(); err == nil {
		t.Fatalf("Recv after Close returned a frame, want a terminal error")
	}
}

// TestLoopback_GetRoundTrip proves a single read crosses the envelope: the
// server builds a registry-typed scan target, the row comes back as opaque
// JSON, and the client scans it into the caller's *testAsset.
func TestLoopback_GetRoundTrip(t *testing.T) {
	fake := &fakeFabric{rel: &fakeRelational{one: &testAsset{ID: "asset-1", TenantID: "acme", Version: 3, Name: "Pump A"}}}
	client := wire(t, fake)

	var got testAsset
	if err := client.Relational().Get(context.Background(), "asset", "asset-1", &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != "asset-1" || got.Name != "Pump A" || got.Version != 3 {
		t.Fatalf("got = %+v, want {asset-1 acme 3 Pump A}", got)
	}
}

// TestLoopback_GetManyRoundTrip proves the batched read returns N rows in order
// (the dataloader / no-N+1 primitive).
func TestLoopback_GetManyRoundTrip(t *testing.T) {
	fake := &fakeFabric{rel: &fakeRelational{many: []*testAsset{
		{ID: "a0", Name: "A"},
		{ID: "a1", Name: "B"},
	}}}
	client := wire(t, fake)

	var got []*testAsset
	if err := client.Relational().GetMany(context.Background(), "asset", []string{"a0", "a1"}, &got); err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 2 || got[0].ID != "a0" || got[1].Name != "B" {
		t.Fatalf("got = %+v, want [a0/A a1/B]", got)
	}
}

// TestLoopback_GetNotFoundSurvivesWire proves the read plane carries the typed
// error taxonomy too: a missing id comes back as errors.Is(ErrNotFound).
func TestLoopback_GetNotFoundSurvivesWire(t *testing.T) {
	fake := &fakeFabric{rel: &fakeRelational{err: fabriqerr.ErrNotFound}}
	client := wire(t, fake)

	var got testAsset
	err := client.Relational().Get(context.Background(), "asset", "missing", &got)
	if !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("Get err = %v, want ErrNotFound", err)
	}
}

// TestLoopback_ListRoundTrip proves the structured filter survives the wire: an
// Eq leaf, a nested Or(Like, ILike) group, and the paging fields all arrive at
// the server intact, and the page scans back into the caller's slice.
func TestLoopback_ListRoundTrip(t *testing.T) {
	rel := &fakeRelational{many: []*testAsset{{ID: "a0", Name: "A"}, {ID: "a1", Name: "B"}}}
	client := wire(t, &fakeFabric{rel: rel})

	q := query.ListQuery{
		Where: query.Where{
			query.Eq("status", "active"),
			query.Or(query.Like("name", "P%"), query.ILike("name", "q%")),
		},
		OrderBy: "name DESC",
		Limit:   10,
		Offset:  5,
	}
	var got []*testAsset
	if err := client.Relational().List(context.Background(), "asset", q, &got); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[1].Name != "B" {
		t.Fatalf("rows = %+v, want [a0/A a1/B]", got)
	}

	gl := rel.gotList
	if gl.OrderBy != "name DESC" || gl.Limit != 10 || gl.Offset != 5 {
		t.Fatalf("paging lost: %+v", gl)
	}
	if len(gl.Where) != 2 {
		t.Fatalf("where lost: %+v", gl.Where)
	}
	if eq := gl.Where[0]; eq.Column != "status" || eq.Op != query.OpEq || eq.Value != "active" {
		t.Fatalf("eq cond lost: %+v", eq)
	}
	if or := gl.Where[1]; !or.IsGroup() || len(or.Or) != 2 ||
		or.Or[0].Op != query.OpLike || or.Or[1].Op != query.OpILike {
		t.Fatalf("OR group lost: %+v", gl.Where[1])
	}
}

// TestLoopback_ListFilterPreservesIntType proves the documented int→float64
// fidelity loss (opaque-JSON filter, JSON has no int) is fixed: a filter value
// set as int64 arrives server-side still as int64, not float64.
func TestLoopback_ListFilterPreservesIntType(t *testing.T) {
	rel := &fakeRelational{}
	rf := remote.New(remote.Loopback{Handler: remote.NewHandler(&fakeFabric{rel: rel}, assetRegistry(t))})

	var out []*testAsset
	q := query.ListQuery{Where: query.Where{query.Eq("version", int64(7))}, Limit: 10}
	if err := rf.Relational().List(context.Background(), "asset", q, &out); err != nil {
		t.Fatalf("List: %v", err)
	}
	got := rel.gotList.Where[0].Value
	if _, ok := got.(int64); !ok {
		t.Fatalf("filter value crossed as %T (%v), want int64 — numeric fidelity lost", got, got)
	}
}

// TestLoopback_ListFilterPreservesInSlice proves that a query.In/NotIn filter
// built with a real typed slice (e.g. []string, not []any) survives the wire
// as a multi-element list rather than being flattened by the string fallback
// into a single fmt.Sprint(v) scalar.
func TestLoopback_ListFilterPreservesInSlice(t *testing.T) {
	rel := &fakeRelational{}
	rf := remote.New(remote.Loopback{Handler: remote.NewHandler(&fakeFabric{rel: rel}, assetRegistry(t))})

	var out []*testAsset
	q := query.ListQuery{Where: query.Where{query.In("kind", []string{"pump", "valve"})}}
	if err := rf.Relational().List(context.Background(), "asset", q, &out); err != nil {
		t.Fatalf("List: %v", err)
	}
	got := rel.gotList.Where[0].Value
	rv := reflect.ValueOf(got)
	if rv.Kind() != reflect.Slice || rv.Len() != 2 {
		t.Fatalf("In filter value crossed as %T (%v), want a 2-element slice", got, got)
	}
	if fmt.Sprint(rv.Index(0).Interface()) != "pump" || fmt.Sprint(rv.Index(1).Interface()) != "valve" {
		t.Fatalf("In filter elements = %v, want [pump valve]", got)
	}
}

// fabricNoLive is a facade that does NOT implement LiveQuerier — used to prove
// the remote LiveQuery degrades to ErrNotImplemented.
type fabricNoLive struct{ query.Fabric }

// TestLoopback_LiveQueryStreamsSnapshotAndDeltas proves the maintained-window
// plane: the snapshot arrives synchronously, then deltas stream in order, and
// the channel closes when the subscription ends. Reanchor is deferred (bidi).
func TestLoopback_LiveQueryStreamsSnapshotAndDeltas(t *testing.T) {
	deltas := make(chan livequery.LiveDelta, 2)
	deltas <- livequery.LiveDelta{Op: livequery.OpEnter, AggID: "a"}
	deltas <- livequery.LiveDelta{Op: livequery.OpUpdate, AggID: "a", Version: 2}
	close(deltas)
	snap := livequery.Snapshot{
		SubID:     "s1",
		Watermark: "w1",
		Rows:      []livequery.Row{{AggID: "a", Version: 1}},
	}
	client := wire(t, &fakeFabric{liveSnap: snap, liveCh: deltas})

	gotSnap, live, h, err := client.LiveQuery(context.Background(), livequery.LiveQuery{Entity: "asset", Limit: 10})
	if err != nil {
		t.Fatalf("LiveQuery: %v", err)
	}
	defer h.Close()
	if gotSnap.SubID != "s1" || gotSnap.Watermark != "w1" || len(gotSnap.Rows) != 1 || gotSnap.Rows[0].AggID != "a" {
		t.Fatalf("snapshot lost: %+v", gotSnap)
	}
	ops := make([]livequery.DeltaOp, 0, 2)
	for d := range live {
		ops = append(ops, d.Op)
	}
	if len(ops) != 2 || ops[0] != livequery.OpEnter || ops[1] != livequery.OpUpdate {
		t.Fatalf("deltas lost: %+v", ops)
	}
}

// TestLoopback_LiveQueryReanchorNotConfigured proves that Reanchor on a live
// stream whose facade returns no engine subscription is answered over the wire
// with ErrNotImplemented (the delta channel stays open so the reply is not
// racing a stream teardown).
func TestLoopback_LiveQueryReanchorNotConfigured(t *testing.T) {
	deltas := make(chan livequery.LiveDelta) // never closed: stream stays open
	client := wire(t, &fakeFabric{liveSnap: livequery.Snapshot{SubID: "s1"}, liveCh: deltas})

	_, _, h, err := client.LiveQuery(context.Background(), livequery.LiveQuery{Entity: "asset", Limit: 10})
	if err != nil {
		t.Fatalf("LiveQuery: %v", err)
	}
	defer h.Close()
	if _, rerr := h.Reanchor(context.Background(), nil, 20); !errors.Is(rerr, remote.ErrNotImplemented) {
		t.Fatalf("Reanchor err = %v, want ErrNotImplemented", rerr)
	}
}

// TestLoopback_LiveQueryNotConfigured proves a facade without LiveQuery degrades
// cleanly: the remote call returns errors.Is(ErrNotImplemented), not a panic.
func TestLoopback_LiveQueryNotConfigured(t *testing.T) {
	h := remote.NewHandler(fabricNoLive{}, assetRegistry(t))
	client := remote.New(remote.Loopback{Handler: h})

	_, _, _, err := client.LiveQuery(context.Background(), livequery.LiveQuery{Entity: "asset", Limit: 10})
	if !errors.Is(err, remote.ErrNotImplemented) {
		t.Fatalf("err = %v, want ErrNotImplemented", err)
	}
}

// TestLoopback_LiveQueryReanchor proves the bidirectional Reanchor round-trip:
// a control frame with a new cursor+limit crosses mid-stream, the fake
// subscription records it, and the fresh snapshot returns to the caller while
// deltas keep flowing on the delta channel.
func TestLoopback_LiveQueryReanchor(t *testing.T) {
	deltas := make(chan livequery.LiveDelta) // unbuffered: stays open across the reanchor
	sub := &fakeLiveSub{reanchSnap: livequery.Snapshot{SubID: "s2", Watermark: "w2", Rows: []livequery.Row{{AggID: "b", Version: 5}}}}
	snap := livequery.Snapshot{SubID: "s1", Watermark: "w1", Rows: []livequery.Row{{AggID: "a", Version: 1}}}
	client := wire(t, &fakeFabric{liveSnap: snap, liveCh: deltas, liveSub: sub})

	gotSnap, live, h, err := client.LiveQuery(context.Background(), livequery.LiveQuery{Entity: "asset", Limit: 10})
	if err != nil {
		t.Fatalf("LiveQuery: %v", err)
	}
	defer h.Close()
	if gotSnap.SubID != "s1" {
		t.Fatalf("initial snapshot lost: %+v", gotSnap)
	}

	// A delta arrives; then we reanchor; then another delta — proving the delta
	// channel survives the reanchor.
	go func() { deltas <- livequery.LiveDelta{Op: livequery.OpEnter, AggID: "a"} }()
	if d := <-live; d.Op != livequery.OpEnter {
		t.Fatalf("pre-reanchor delta = %+v", d)
	}

	cursor := &livequery.Cursor{Values: []any{"anchor", "b"}}
	fresh, err := h.Reanchor(context.Background(), cursor, 20)
	if err != nil {
		t.Fatalf("Reanchor: %v", err)
	}
	if fresh.SubID != "s2" || fresh.Watermark != "w2" || len(fresh.Rows) != 1 || fresh.Rows[0].AggID != "b" {
		t.Fatalf("reanchor snapshot lost: %+v", fresh)
	}
	sub.mu.Lock()
	gotLimit, hit := sub.gotLimit, sub.reanchorHit
	gotCursor := sub.gotCursor
	sub.mu.Unlock()
	if hit != 1 || gotLimit != 20 {
		t.Fatalf("engine saw hit=%d limit=%d, want 1 and 20", hit, gotLimit)
	}
	if gotCursor == nil || len(gotCursor.Values) != 2 || fmt.Sprint(gotCursor.Values[0]) != "anchor" {
		t.Fatalf("engine saw cursor = %+v, want values [anchor b]", gotCursor)
	}

	go func() { deltas <- livequery.LiveDelta{Op: livequery.OpUpdate, AggID: "b", Version: 6} }()
	if d := <-live; d.Op != livequery.OpUpdate {
		t.Fatalf("post-reanchor delta = %+v", d)
	}
}

// TestLoopback_BlobPutGetRoundTrip proves the byte plane: a multi-chunk body
// streams up and back down byte-for-byte (never buffered whole on the wire).
func TestLoopback_BlobPutGetRoundTrip(t *testing.T) {
	client := wire(t, &fakeFabric{blobStore: &fakeBlob{}})
	b := client.Blob()

	body := bytes.Repeat([]byte("xyz "), 100_000) // ~400 KiB → multiple 256 KiB frames
	info, err := b.Put(context.Background(), "k1", bytes.NewReader(body),
		blob.PutOpts{ContentType: "text/plain", Size: int64(len(body))})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if info.Key != "k1" || info.Size != int64(len(body)) {
		t.Fatalf("put info = %+v", info)
	}

	rc, gi, err := b.Get(context.Background(), "k1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch: got %d bytes, want %d", len(got), len(body))
	}
	if gi.Size != int64(len(body)) {
		t.Fatalf("get info size = %d, want %d", gi.Size, len(body))
	}
}

// TestLoopback_BlobGetNotFound proves the byte plane carries the typed error
// taxonomy: a missing key comes back as errors.Is(ErrNotFound).
func TestLoopback_BlobGetNotFound(t *testing.T) {
	client := wire(t, &fakeFabric{blobStore: &fakeBlob{}})
	_, _, err := client.Blob().Get(context.Background(), "missing")
	if !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("Get err = %v, want ErrNotFound", err)
	}
}

// TestLoopback_BlobPresign proves the presign bypass: the remote store is a
// blob.Presigner and returns the server's signed URL.
func TestLoopback_BlobPresign(t *testing.T) {
	client := wire(t, &fakeFabric{blobStore: &fakeBlob{}})
	ps, ok := client.Blob().(blob.Presigner)
	if !ok {
		t.Fatal("remote blob is not a Presigner")
	}
	url, err := ps.PresignGet(context.Background(), "k1", time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	if url != "https://store/get/k1" {
		t.Fatalf("url = %q", url)
	}
}

// TestLoopback_BlobListCopyRoundTrip proves the List/Copy unary methods cross
// the envelope: List returns the canned page, Copy returns the canned result.
func TestLoopback_BlobListCopyRoundTrip(t *testing.T) {
	fb := &fakeBlob{
		listResult: []blob.ObjectInfo{{Key: "a", Size: 3}},
		copyResult: blob.ObjectInfo{Key: "b", Size: 3},
	}
	client := wire(t, &fakeFabric{blobStore: fb})

	got, err := client.Blob().List(context.Background(), "pre/")
	if err != nil || len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("List = %+v %v", got, err)
	}
	ci, err := client.Blob().Copy(context.Background(), "a", "b")
	if err != nil || ci.Key != "b" {
		t.Fatalf("Copy = %+v %v", ci, err)
	}
}

// TestLoopback_VectorSimilar proves the semantic channel: VectorQuery crosses
// and []VectorMatch scans back.
func TestLoopback_VectorSimilar(t *testing.T) {
	retr := &fakeRetrieval{matches: []query.VectorMatch{{ID: "a", Score: 0.9}, {ID: "b", Score: 0.8}}}
	client := wire(t, &fakeFabric{retr: retr})

	var got []query.VectorMatch
	if err := client.Vector().Similar(context.Background(),
		query.VectorQuery{Entity: "asset", Embedding: []float32{0.1, 0.2}, K: 5}, &got); err != nil {
		t.Fatalf("Similar: %v", err)
	}
	if len(got) != 2 || got[0].ID != "a" || got[1].Score != 0.8 {
		t.Fatalf("matches lost: %+v", got)
	}
	if retr.gotVecQ.Entity != "asset" || retr.gotVecQ.K != 5 || len(retr.gotVecQ.Embedding) != 2 {
		t.Fatalf("vector query lost: %+v", retr.gotVecQ)
	}
}

// TestLoopback_Search proves the lexical channel: SearchQuery (incl. its Where
// filter) crosses and the hit maps scan back.
func TestLoopback_Search(t *testing.T) {
	retr := &fakeRetrieval{hits: []map[string]any{{"id": "a"}, {"id": "b"}}}
	client := wire(t, &fakeFabric{retr: retr})

	var got []map[string]any
	if err := client.Search().Search(context.Background(),
		query.SearchQuery{Entity: "asset", Query: "pump", Limit: 10}, &got); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 2 || got[0]["id"] != "a" {
		t.Fatalf("hits lost: %+v", got)
	}
	if retr.gotSearchQ.Query != "pump" || retr.gotSearchQ.Limit != 10 {
		t.Fatalf("search query lost: %+v", retr.gotSearchQ)
	}
}

// TestLoopback_GraphQueryIDs proves the graph channel's id-traversal shape
// (what recall's graph-expand uses): *[]string round-trips.
func TestLoopback_GraphQueryIDs(t *testing.T) {
	retr := &fakeRetrieval{ids: []string{"x", "y"}}
	client := wire(t, &fakeFabric{retr: retr})

	var got []string
	if err := client.Graph().Query(context.Background(), "MATCH (n)-[:R]->(m) RETURN m.id",
		map[string]any{"id": "seed"}, &got); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("ids lost: %+v", got)
	}
	if retr.gotCypher == "" {
		t.Fatal("cypher not forwarded")
	}
}

// TestLoopback_GraphQueryRows proves the multi-column shape: *[]map round-trips.
func TestLoopback_GraphQueryRows(t *testing.T) {
	retr := &fakeRetrieval{rows: []map[string]any{{"name": "a"}}}
	client := wire(t, &fakeFabric{retr: retr})

	var got []map[string]any
	if err := client.Graph().Query(context.Background(), "MATCH (n) RETURN n", nil, &got); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 1 || got[0]["name"] != "a" {
		t.Fatalf("rows lost: %+v", got)
	}
}

// TestLoopback_VectorGetRoundTrip proves Vector().Get over the Loopback wire:
//   - Upsert a vector then Get it back — the returned []float32 must match.
//   - Get a missing id — errors.Is(err, fabriqerr.ErrNotFound) must hold, proving
//     the NotFound taxonomy survives the VectorGetReply error envelope.
//
// Implementation note: the fakeRetrieval.vecStore map stands in for the real
// vector store; Upsert writes into it, Get reads from it. The Wire transport
// used by this test drives the full client→proto→server→proto→client round trip
// via the in-process Loopback transport.
func TestLoopback_VectorGetRoundTrip(t *testing.T) {
	retr := &fakeRetrieval{vecStore: map[string][]float32{}}
	client := wire(t, &fakeFabric{retr: retr})
	ctx := context.Background()

	want := []float32{0.1, 0.2, 0.3}
	if err := client.Vector().Upsert(ctx, "asset", "v1", want, nil); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := client.Vector().Get(ctx, "asset", "v1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("Get returned %d floats, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d] = %v, want %v", i, got[i], want[i])
		}
	}

	// Missing id must return ErrNotFound through the wire.
	_, err = client.Vector().Get(ctx, "asset", "missing")
	if !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("Get missing err = %v, want ErrNotFound", err)
	}
}

// TestLoopback_VectorSimilarFilter proves that a non-empty Filter crosses the
// envelope and arrives at the server: the filter map is JSON-marshalled on the
// client side, carried in VectorSimilarRequest.filter, and unmarshalled back
// into VectorQuery.Filter before the real port is called.
func TestLoopback_VectorSimilarFilter(t *testing.T) {
	retr := &fakeRetrieval{matches: []query.VectorMatch{{ID: "x", Score: 0.95}}}
	client := wire(t, &fakeFabric{retr: retr})

	filter := map[string]string{"kind": "pump", "region": "eu"}
	var got []query.VectorMatch
	if err := client.Vector().Similar(context.Background(),
		query.VectorQuery{Entity: "asset", Embedding: []float32{0.5}, K: 3, Filter: filter}, &got); err != nil {
		t.Fatalf("Similar with filter: %v", err)
	}
	if len(got) != 1 || got[0].ID != "x" {
		t.Fatalf("matches lost: %+v", got)
	}
	// The server must have received the filter intact.
	if retr.gotVecQ.Filter["kind"] != "pump" || retr.gotVecQ.Filter["region"] != "eu" {
		t.Fatalf("filter not forwarded: %+v", retr.gotVecQ.Filter)
	}
}

// TestLoopback_VectorDeleteByMeta proves the DeleteByMeta RPC path end-to-end:
// the filter map is marshalled by the client, carried in
// VectorDeleteByMetaRequest.filter, unmarshalled by the server, and forwarded
// to the backing store with entity and filter intact.
func TestLoopback_VectorDeleteByMeta(t *testing.T) {
	retr := &fakeRetrieval{}
	client := wire(t, &fakeFabric{retr: retr})

	filter := map[string]string{"owner": "acme", "model": "v2"}
	if err := client.Vector().DeleteByMeta(context.Background(), "asset", filter); err != nil {
		t.Fatalf("DeleteByMeta: %v", err)
	}
	if retr.gotDelByMetaEntity != "asset" {
		t.Errorf("entity = %q, want %q", retr.gotDelByMetaEntity, "asset")
	}
	if retr.gotDelByMetaFilter["owner"] != "acme" || retr.gotDelByMetaFilter["model"] != "v2" {
		t.Fatalf("filter not forwarded: %+v", retr.gotDelByMetaFilter)
	}
}

// TestLoopback_RecallChannelsAllWired proves the whole point: every channel
// recall fuses — semantic, lexical, graph→ids, and id hydration — works over
// the remote transport (none returns ErrNotImplemented).
func TestLoopback_RecallChannelsAllWired(t *testing.T) {
	retr := &fakeRetrieval{
		matches: []query.VectorMatch{{ID: "v1"}},
		hits:    []map[string]any{{"id": "s1"}},
		ids:     []string{"g1"},
	}
	client := wire(t, &fakeFabric{
		retr: retr,
		rel:  &fakeRelational{many: []*testAsset{{ID: "g1", Name: "Pump"}}},
	})
	ctx := context.Background()

	var vm []query.VectorMatch
	if err := client.Vector().Similar(ctx, query.VectorQuery{Entity: "asset", Embedding: []float32{1}, K: 3}, &vm); err != nil {
		t.Fatalf("semantic channel: %v", err)
	}
	var hits []map[string]any
	if err := client.Search().Search(ctx, query.SearchQuery{Entity: "asset", Query: "q"}, &hits); err != nil {
		t.Fatalf("lexical channel: %v", err)
	}
	var ids []string
	if err := client.Graph().Query(ctx, "MATCH … RETURN id", map[string]any{"id": "seed"}, &ids); err != nil {
		t.Fatalf("graph channel: %v", err)
	}
	var hydrated []*testAsset
	if err := client.Relational().GetMany(ctx, "asset", ids, &hydrated); err != nil {
		t.Fatalf("id hydration: %v", err)
	}
	if len(vm) != 1 || len(hits) != 1 || len(ids) != 1 || len(hydrated) != 1 || hydrated[0].Name != "Pump" {
		t.Fatalf("a channel dropped: vm=%v hits=%v ids=%v hydrated=%v", vm, hits, ids, hydrated)
	}
}

// TestLoopback_TimeseriesRoundTrip proves the telemetry port: BulkWrite's
// points cross to the server and Range's rows scan back, over the Loopback
// wire.
func TestLoopback_TimeseriesRoundTrip(t *testing.T) {
	ts := &fakeTS{rows: []map[string]any{{"value": 1.5}}}
	fab := &fakeFabric{ts: ts}
	rf := remote.New(remote.Loopback{Handler: remote.NewHandler(fab, assetRegistry(t))})

	at := time.Unix(1700000000, 0).UTC()
	if err := rf.Timeseries().BulkWrite(context.Background(), "tags",
		[]query.Point{{Key: "t1", At: at, Value: 1.5, Quality: 1}}); err != nil {
		t.Fatalf("BulkWrite: %v", err)
	}
	if len(ts.gotPoints) != 1 || ts.gotPoints[0].Key != "t1" || ts.gotPoints[0].Value != 1.5 {
		t.Fatalf("server saw points %+v", ts.gotPoints)
	}

	var out []map[string]any
	if err := rf.Timeseries().Range(context.Background(),
		query.RangeQuery{Series: "tags", Key: "t1", From: at, Bucket: time.Minute, Agg: "avg"}, &out); err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(out) != 1 || out[0]["value"] != 1.5 {
		t.Fatalf("rows = %+v", out)
	}
	if ts.gotRange.Agg != "avg" || ts.gotRange.Bucket != time.Minute {
		t.Fatalf("server saw range %+v", ts.gotRange)
	}
}

// TestLoopback_SpatialRoundTrip proves the geometry port: Upsert's geometry
// crosses to the server and Get's canned geometry/meta/ok scan back, over the
// Loopback wire.
func TestLoopback_SpatialRoundTrip(t *testing.T) {
	sp := &fakeSpatial{getGeom: query.Geometry{WKT: "POINT(1 2)", SRID: 4326}, getMeta: map[string]any{"n": "a"}, getOK: true}
	rf := remote.New(remote.Loopback{Handler: remote.NewHandler(&fakeFabric{sp: sp}, assetRegistry(t))})

	if err := rf.Spatial().Upsert(context.Background(), "asset", "a1",
		query.Geometry{WKT: "POINT(3 4)", SRID: 4326}, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if sp.gotGeom.WKT != "POINT(3 4)" {
		t.Fatalf("server geom = %+v", sp.gotGeom)
	}
	geom, meta, ok, err := rf.Spatial().Get(context.Background(), "asset", "a1")
	if err != nil || !ok || geom.WKT != "POINT(1 2)" || meta["n"] != "a" {
		t.Fatalf("Get = %+v %+v %v %v", geom, meta, ok, err)
	}
}

// TestLoopback_DocumentRoundTrip proves the document plane: ApplyUpdate's
// bytes cross to the server, Sync round-trips the state vector and the
// programmed update, Snapshot returns the programmed Materialized, and
// Compact reaches the server — all over the Loopback wire.
func TestLoopback_DocumentRoundTrip(t *testing.T) {
	doc := &fakeDocStore{
		syncUpdate: []byte("update-bytes"),
		snapshot: document.Materialized{
			DocID:    "doc-1",
			Version:  3,
			Snapshot: json.RawMessage(`{"title":"hi"}`),
		},
	}
	rf := remote.New(remote.Loopback{Handler: remote.NewHandler(&fakeFabric{doc: doc}, assetRegistry(t))})

	if rf.Document() == nil {
		t.Fatal("Document() is nil")
	}

	if err := rf.Document().ApplyUpdate(context.Background(), "doc-1", []byte("update-1")); err != nil {
		t.Fatalf("ApplyUpdate: %v", err)
	}
	if doc.gotApplyDocID != "doc-1" || string(doc.gotApplyUpdate) != "update-1" {
		t.Fatalf("server saw ApplyUpdate(%q, %q)", doc.gotApplyDocID, doc.gotApplyUpdate)
	}

	upd, err := rf.Document().Sync(context.Background(), "doc-1", []byte("sv-1"))
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if string(upd) != "update-bytes" {
		t.Fatalf("Sync update = %q", upd)
	}
	if doc.gotSyncDocID != "doc-1" || string(doc.gotSyncSV) != "sv-1" {
		t.Fatalf("server saw Sync(%q, %q)", doc.gotSyncDocID, doc.gotSyncSV)
	}

	mat, err := rf.Document().Snapshot(context.Background(), "doc-1")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if mat.DocID != "doc-1" || mat.Version != 3 || string(mat.Snapshot) != `{"title":"hi"}` {
		t.Fatalf("Snapshot = %+v", mat)
	}

	if err := rf.Document().Compact(context.Background(), "doc-1"); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if doc.gotCompactDoc != "doc-1" {
		t.Fatalf("server saw Compact(%q)", doc.gotCompactDoc)
	}
}
