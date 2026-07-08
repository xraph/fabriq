package remotegrpc_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xraph/grove"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/remote"
	remotegrpc "github.com/xraph/fabriq/remote/grpc"
)

type asset struct {
	grove.BaseModel `grove:"table:assets"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
}

func assetRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	r := registry.New()
	if err := r.Register(registry.EntitySpec{Name: "asset", Kind: registry.KindAggregate, Model: (*asset)(nil)}); err != nil {
		t.Fatalf("register: %v", err)
	}
	return r
}

// fakeFabric is the embedded facade the gRPC server delegates to. Exec records
// the tenant it sees so auth tests can assert the edge-stamped identity reached
// the facade.
type fakeFabric struct {
	query.Fabric
	result       command.Result
	err          error
	subCh        chan query.Delta
	liveSnap     livequery.Snapshot
	liveCh       chan livequery.LiveDelta
	rel          *fakeRelational
	blobStore    blob.Store
	retr         *fakeRetrieval
	ts           *fakeTS
	sp           *fakeSpatial
	gotTenant    string
	gotPrincipal string
}

func (f *fakeFabric) Vector() query.VectorQuerier   { return f.retr }
func (f *fakeFabric) Search() query.SearchQuerier   { return f.retr }
func (f *fakeFabric) Graph() query.GraphQuerier     { return f.retr }
func (f *fakeFabric) Timeseries() query.TSQuerier   { return f.ts }
func (f *fakeFabric) Spatial() query.SpatialQuerier { return f.sp }

// fakeRetrieval implements the three projection-read ports for the gRPC channel
// test.
type fakeRetrieval struct {
	matches []query.VectorMatch
	hits    []map[string]any
	ids     []string
}

func (f *fakeRetrieval) Similar(_ context.Context, _ query.VectorQuery, into any) error {
	if p, ok := into.(*[]query.VectorMatch); ok {
		*p = f.matches
	}
	return nil
}
func (f *fakeRetrieval) Upsert(context.Context, string, string, []float32, map[string]any) error {
	return nil
}
func (f *fakeRetrieval) Delete(context.Context, string, string) error { return nil }
func (f *fakeRetrieval) DeleteByMeta(context.Context, string, map[string]string) error {
	return nil
}
func (f *fakeRetrieval) Get(context.Context, string, string) ([]float32, error) { return nil, nil }
func (f *fakeRetrieval) Search(_ context.Context, _ query.SearchQuery, into any) error {
	if p, ok := into.(*[]map[string]any); ok {
		*p = f.hits
	}
	return nil
}
func (f *fakeRetrieval) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return nil
}
func (f *fakeRetrieval) Query(_ context.Context, _ string, _ map[string]any, into any) error {
	if p, ok := into.(*[]string); ok {
		*p = f.ids
	}
	return nil
}
func (f *fakeRetrieval) TraverseAndHydrate(context.Context, string, map[string]any, any) error {
	return nil
}

// fakeTS implements query.TSQuerier for the gRPC timeseries round-trip test.
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

// fakeSpatial implements query.SpatialQuerier for the gRPC spatial round-trip
// test.
type fakeSpatial struct {
	gotGeom query.Geometry
	gotMeta map[string]any
	getGeom query.Geometry
	getMeta map[string]any
	getOK   bool
}

func (f *fakeSpatial) Upsert(_ context.Context, _, _ string, geom query.Geometry, meta map[string]any) error {
	f.gotGeom = geom
	f.gotMeta = meta
	return nil
}
func (f *fakeSpatial) Within(context.Context, query.SpatialQuery, any) error { return nil }
func (f *fakeSpatial) Get(_ context.Context, _, _ string) (query.Geometry, map[string]any, bool, error) {
	return f.getGeom, f.getMeta, f.getOK, nil
}
func (f *fakeSpatial) Delete(_ context.Context, _, _ string) error { return nil }

func (f *fakeFabric) LiveQuery(_ context.Context, _ livequery.LiveQuery) (livequery.Snapshot, <-chan livequery.LiveDelta, *livequery.Handle, error) {
	return f.liveSnap, f.liveCh, nil, nil
}

func (f *fakeFabric) Blob() blob.Store { return f.blobStore }

// fakeBlob is a minimal in-memory blob.Store for the byte-plane gRPC test.
type fakeBlob struct {
	mu   sync.Mutex
	data map[string][]byte
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
	return blob.ObjectInfo{Key: key, Size: int64(len(body)), ContentType: o.ContentType}, nil
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

func (b *fakeBlob) Head(context.Context, string) (blob.ObjectInfo, error) {
	return blob.ObjectInfo{}, fabriqerr.ErrNotFound
}
func (b *fakeBlob) Delete(context.Context, string) error                    { return nil }
func (b *fakeBlob) List(context.Context, string) ([]blob.ObjectInfo, error) { return nil, nil }
func (b *fakeBlob) Copy(context.Context, string, string) (blob.ObjectInfo, error) {
	return blob.ObjectInfo{}, nil
}
func (b *fakeBlob) Capabilities() blob.Caps { return blob.Caps{} }
func (b *fakeBlob) PresignGet(context.Context, string, time.Duration) (string, error) {
	return "", nil
}
func (b *fakeBlob) PresignPut(context.Context, string, time.Duration) (string, error) {
	return "", nil
}

func (f *fakeFabric) Exec(ctx context.Context, _ command.Command) (command.Result, error) {
	f.gotTenant, _ = tenant.Require(ctx)
	f.gotPrincipal = principalFrom(ctx)
	return f.result, f.err
}

func (f *fakeFabric) Subscribe(_ context.Context, _ query.SubscribeScope) (<-chan query.Delta, error) {
	return f.subCh, nil
}

func (f *fakeFabric) Relational() query.RelationalQuerier { return f.rel }

// fakeRelational is a minimal relational port for the read tests.
type fakeRelational struct {
	many    []*asset
	gotList query.ListQuery
}

func (f *fakeRelational) Get(context.Context, string, string, any) error       { return nil }
func (f *fakeRelational) GetMany(context.Context, string, []string, any) error { return nil }
func (f *fakeRelational) Query(context.Context, any, string, ...any) error     { return nil }

func (f *fakeRelational) List(_ context.Context, _ string, q query.ListQuery, into any) error {
	f.gotList = q
	if p, ok := into.(*[]*asset); ok {
		*p = f.many
	}
	return nil
}

// dial stands up a real gRPC server over an in-memory bufconn listener and
// returns a Fabric talking to it — the whole stack except the network
// socket, exercising actual gRPC framing/streaming.
func dial(t *testing.T, fab query.Fabric, opts ...grpc.ServerOption) *remote.Fabric {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(opts...)
	remotegrpc.Register(srv, remote.NewHandler(fab, assetRegistry(t)))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return remote.New(remotegrpc.NewClient(cc))
}

// TestGRPC_ExecRoundTrip proves a write crosses a real gRPC connection: the
// typed payload is decoded server-side and the result returns intact.
func TestGRPC_ExecRoundTrip(t *testing.T) {
	fab := dial(t, &fakeFabric{result: command.Result{AggID: "asset-1", Version: 1, EventID: "evt-1"}})

	res, err := fab.Exec(context.Background(), command.Command{
		Entity: "asset", Op: command.OpCreate,
		Payload: &asset{ID: "asset-1", TenantID: "acme", Name: "Pump A"},
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.AggID != "asset-1" || res.Version != 1 || res.EventID != "evt-1" {
		t.Fatalf("res = %+v, want {asset-1 1 evt-1}", res)
	}
}

// TestGRPC_VersionConflictSurvivesWire proves the in-band typed-error taxonomy
// survives a real gRPC round trip (the error rides inside the reply envelope,
// not as a gRPC status).
func TestGRPC_VersionConflictSurvivesWire(t *testing.T) {
	fab := dial(t, &fakeFabric{err: &fabriqerr.VersionConflictError{Aggregate: "asset", AggID: "x", Expected: 1, Actual: 2}})

	_, err := fab.Exec(context.Background(), command.Command{
		Entity: "asset", Op: command.OpUpdate, AggID: "x", Payload: &asset{ID: "x"},
	})
	if !errors.Is(err, fabriqerr.ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
}

// TestGRPC_ListRoundTrip proves the structured filter crosses real gRPC and the
// page scans back — and that List is actually registered in the ServiceDesc
// (not just reachable over Loopback).
func TestGRPC_ListRoundTrip(t *testing.T) {
	rel := &fakeRelational{many: []*asset{{ID: "a0", Name: "A"}, {ID: "a1", Name: "B"}}}
	fab := dial(t, &fakeFabric{rel: rel})

	var got []*asset
	q := query.ListQuery{Where: query.Where{query.Eq("status", "active")}, OrderBy: "name", Limit: 5}
	if err := fab.Relational().List(context.Background(), "asset", q, &got); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[1].Name != "B" {
		t.Fatalf("rows = %+v, want [a0/A a1/B]", got)
	}
	if rel.gotList.OrderBy != "name" || rel.gotList.Limit != 5 ||
		len(rel.gotList.Where) != 1 || rel.gotList.Where[0].Column != "status" {
		t.Fatalf("filter lost over gRPC: %+v", rel.gotList)
	}
}

// TestGRPC_RecallChannels proves the agent toolkit's three retrieval channels
// (semantic / lexical / graph) all work over real gRPC — none ErrNotImplemented.
func TestGRPC_RecallChannels(t *testing.T) {
	fab := dial(t, &fakeFabric{retr: &fakeRetrieval{
		matches: []query.VectorMatch{{ID: "v1"}},
		hits:    []map[string]any{{"id": "s1"}},
		ids:     []string{"g1"},
	}})
	ctx := context.Background()

	var vm []query.VectorMatch
	if err := fab.Vector().Similar(ctx, query.VectorQuery{Entity: "asset", Embedding: []float32{1}, K: 3}, &vm); err != nil {
		t.Fatalf("Vector.Similar over gRPC: %v", err)
	}
	var hits []map[string]any
	if err := fab.Search().Search(ctx, query.SearchQuery{Entity: "asset", Query: "q"}, &hits); err != nil {
		t.Fatalf("Search over gRPC: %v", err)
	}
	var ids []string
	if err := fab.Graph().Query(ctx, "MATCH ... RETURN id", map[string]any{"id": "seed"}, &ids); err != nil {
		t.Fatalf("Graph.Query over gRPC: %v", err)
	}
	if len(vm) != 1 || vm[0].ID != "v1" || len(hits) != 1 || hits[0]["id"] != "s1" || len(ids) != 1 || ids[0] != "g1" {
		t.Fatalf("channel lost over gRPC: vm=%v hits=%v ids=%v", vm, hits, ids)
	}
}

// TestGRPC_BlobPutGetRoundTrip proves the byte plane over real gRPC: a
// multi-chunk body streams up (client-streaming) and back down (server-streaming)
// byte-for-byte.
func TestGRPC_BlobPutGetRoundTrip(t *testing.T) {
	fab := dial(t, &fakeFabric{blobStore: &fakeBlob{}})
	b := fab.Blob()

	body := bytes.Repeat([]byte("blob "), 80_000) // ~400 KiB → multiple 256 KiB frames
	if _, err := b.Put(context.Background(), "k1", bytes.NewReader(body), blob.PutOpts{Size: int64(len(body))}); err != nil {
		t.Fatalf("Put over gRPC: %v", err)
	}
	rc, _, err := b.Get(context.Background(), "k1")
	if err != nil {
		t.Fatalf("Get over gRPC: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch over gRPC: got %d bytes, want %d", len(got), len(body))
	}
}

// TestGRPC_LiveQueryStreamsSnapshotAndDeltas proves the maintained-window plane
// over real gRPC: the snapshot returns synchronously, then deltas stream.
func TestGRPC_LiveQueryStreamsSnapshotAndDeltas(t *testing.T) {
	deltas := make(chan livequery.LiveDelta, 2)
	deltas <- livequery.LiveDelta{Op: livequery.OpEnter, AggID: "a"}
	deltas <- livequery.LiveDelta{Op: livequery.OpMove, AggID: "a", NewIndex: 1}
	close(deltas)
	fab := dial(t, &fakeFabric{
		liveSnap: livequery.Snapshot{SubID: "s1", Rows: []livequery.Row{{AggID: "a", Version: 1}}},
		liveCh:   deltas,
	})

	snap, live, h, err := fab.LiveQuery(context.Background(), livequery.LiveQuery{Entity: "asset", Limit: 10})
	if err != nil {
		t.Fatalf("LiveQuery: %v", err)
	}
	defer h.Close()
	if snap.SubID != "s1" || len(snap.Rows) != 1 || snap.Rows[0].AggID != "a" {
		t.Fatalf("snapshot lost over gRPC: %+v", snap)
	}
	ops := make([]livequery.DeltaOp, 0, 2)
	for d := range live {
		ops = append(ops, d.Op)
	}
	if len(ops) != 2 || ops[0] != livequery.OpEnter || ops[1] != livequery.OpMove {
		t.Fatalf("deltas lost over gRPC: %+v", ops)
	}
}

// TestGRPC_SubscribeStreamsDeltas proves server-streaming over real gRPC: deltas
// arrive in order and the channel closes when the stream ends.
func TestGRPC_SubscribeStreamsDeltas(t *testing.T) {
	ch := make(chan query.Delta, 2)
	ch <- query.Delta{AggID: "a", Type: "updated"}
	ch <- query.Delta{AggID: "b", Type: "created"}
	close(ch)
	fab := dial(t, &fakeFabric{subCh: ch})

	out, err := fab.Subscribe(context.Background(), query.SubscribeScope{Entity: "asset", Scope: "tenant"})
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

// TestGRPC_TimeseriesRoundTrip proves the timeseries port over real gRPC — and
// that TSBulkWrite/TSRange are actually registered in the ServiceDesc (not just
// reachable over Loopback).
func TestGRPC_TimeseriesRoundTrip(t *testing.T) {
	ts := &fakeTS{rows: []map[string]any{{"value": 2.0}}}
	f := dial(t, &fakeFabric{ts: ts})

	if err := f.Timeseries().BulkWrite(context.Background(), "tags",
		[]query.Point{{Key: "t1", At: time.Unix(1, 0), Value: 2.0}}); err != nil {
		t.Fatalf("BulkWrite: %v", err)
	}
	var out []map[string]any
	if err := f.Timeseries().Range(context.Background(), query.RangeQuery{Series: "tags"}, &out); err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(out) != 1 || out[0]["value"] != 2.0 {
		t.Fatalf("rows = %+v", out)
	}
}

// TestGRPC_SpatialRoundTrip proves the spatial port over real gRPC — and that
// SpatialUpsert/Within/Get/Delete are actually registered in the ServiceDesc
// (not just reachable over Loopback).
func TestGRPC_SpatialRoundTrip(t *testing.T) {
	sp := &fakeSpatial{getGeom: query.Geometry{WKT: "POINT(1 2)", SRID: 4326}, getMeta: map[string]any{"n": "a"}, getOK: true}
	f := dial(t, &fakeFabric{sp: sp})

	if err := f.Spatial().Upsert(context.Background(), "asset", "a1",
		query.Geometry{WKT: "POINT(3 4)", SRID: 4326}, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if sp.gotGeom.WKT != "POINT(3 4)" {
		t.Fatalf("server geom = %+v", sp.gotGeom)
	}
	geom, meta, ok, err := f.Spatial().Get(context.Background(), "asset", "a1")
	if err != nil || !ok || geom.WKT != "POINT(1 2)" || meta["n"] != "a" {
		t.Fatalf("Get = %+v %+v %v %v", geom, meta, ok, err)
	}
	if err := f.Spatial().Delete(context.Background(), "asset", "a1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}
