package fabriq

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
)

// fakeSpecStore is an in-memory DynamicSpecStore. Save bumps the revision
// only when the bytes change, like the Postgres adapter's JSONB comparison.
type fakeSpecStore struct {
	rows     map[string]postgres.DynamicSpecRecord
	ops      *[]string
	failSave bool
	saves    int
}

func newFakeSpecStore(ops *[]string) *fakeSpecStore {
	return &fakeSpecStore{rows: map[string]postgres.DynamicSpecRecord{}, ops: ops}
}

func (s *fakeSpecStore) log(op string) {
	if s.ops != nil {
		*s.ops = append(*s.ops, op)
	}
}

func (s *fakeSpecStore) SaveDynamicSpec(_ context.Context, name string, spec []byte) (int64, error) {
	s.log("save:" + name)
	if s.failSave {
		return 0, errors.New("save boom")
	}
	s.saves++
	cur, ok := s.rows[name]
	switch {
	case !ok:
		cur = postgres.DynamicSpecRecord{Name: name, Spec: append([]byte(nil), spec...), Revision: 1}
	case string(cur.Spec) != string(spec):
		cur.Spec = append([]byte(nil), spec...)
		cur.Revision++
	}
	s.rows[name] = cur
	return cur.Revision, nil
}

func (s *fakeSpecStore) DeleteDynamicSpec(_ context.Context, name string) error {
	s.log("delete:" + name)
	delete(s.rows, name)
	return nil
}

func (s *fakeSpecStore) LoadDynamicSpecs(context.Context) ([]postgres.DynamicSpecRecord, error) {
	out := make([]postgres.DynamicSpecRecord, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// put writes a row directly, as another instance would.
func (s *fakeSpecStore) put(t *testing.T, sp registry.EntitySpec, rev int64) {
	t.Helper()
	b, err := encodeDynamicSpec(sp)
	if err != nil {
		t.Fatalf("encode %s: %v", sp.Name, err)
	}
	s.rows[sp.Name] = postgres.DynamicSpecRecord{Name: sp.Name, Spec: b, Revision: rev}
}

func namedSpec(name string) registry.EntitySpec {
	return registry.EntitySpec{
		Name: name, Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{Table: "ds_" + name,
			Columns: []registry.DynamicColumn{{Name: "colour", Type: registry.ColText}}},
	}
}

func (s *fakeSpecStore) lastSaved(t *testing.T, name string) registry.EntitySpec {
	t.Helper()
	r, ok := s.rows[name]
	if !ok {
		t.Fatalf("no saved row for %q", name)
	}
	sp, err := decodeDynamicSpec(r.Spec)
	if err != nil {
		t.Fatalf("decode saved %q: %v", name, err)
	}
	return sp
}

func TestDefineDynamicPersistsSpec(t *testing.T) {
	var ops []string
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(&ops), map[string]int64{}
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec()); err != nil {
		t.Fatalf("define: %v", err)
	}
	if !reflect.DeepEqual(ops, []string{"save:widget"}) {
		t.Fatalf("ops = %v, want [save:widget]", ops)
	}
	got := store.lastSaved(t, "widget")
	want := spec()
	if got.Name != want.Name || got.Schema.Table != want.Schema.Table ||
		!reflect.DeepEqual(got.Schema.Columns, want.Schema.Columns) {
		t.Fatalf("saved spec = %+v / %+v, want %+v / %+v", got, got.Schema, want, want.Schema)
	}
	if loaded["widget"] != 1 {
		t.Fatalf("loaded = %v, want widget:1", loaded)
	}
	if !ddl.guarded["ds_widget"] {
		t.Fatal("table not guarded")
	}
}

func TestDefineDynamicIdempotentWhenPersistedEqual(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec()); err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec()); err != nil {
		t.Fatalf("second define: %v", err)
	}
	if store.saves != 1 {
		t.Fatalf("saves = %d, want 1", store.saves)
	}
	if len(ddl.ensured) != 1 {
		t.Fatalf("ensured = %v, want one EnsureDynamic", ddl.ensured)
	}
}

func TestDefineDynamicIdempotentAttachesInProcessFields(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.put(t, spec(), 1)
	if _, err := syncPersistedDynamic(context.Background(), reg, ddl, store, loaded); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if ent, _ := reg.Get("widget"); ent == nil || ent.Spec.Validate != nil {
		t.Fatal("expected widget loaded without Validate")
	}
	withValidate := spec()
	withValidate.Validate = func(map[string]any) error { return nil }
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, withValidate); err != nil {
		t.Fatalf("define: %v", err)
	}
	if store.saves != 0 {
		t.Fatalf("saves = %d, want 0", store.saves)
	}
	if len(ddl.ensured) != 1 {
		t.Fatalf("ensured = %v, want only the sync's EnsureDynamic", ddl.ensured)
	}
	ent, ok := reg.Get("widget")
	if !ok || ent.Spec.Validate == nil {
		t.Fatal("Validate not attached to the loaded type")
	}
}

func TestDefineDynamicConflictWhenDifferent(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec()); err != nil {
		t.Fatalf("define: %v", err)
	}
	other := spec()
	other.Schema.Columns[0].Type = registry.ColInt // a type change is not additive
	err := defineDynamic(context.Background(), reg, ddl, store, loaded, other)
	if err == nil || !strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("err = %v, want registered twice", err)
	}
	if store.saves != 1 {
		t.Fatalf("saves = %d, want 1", store.saves)
	}
}

func TestDefineDynamicRollsBackOnPersistFailure(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.failSave = true
	err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec())
	if err == nil || !strings.Contains(err.Error(), "persist dynamic entity") {
		t.Fatalf("err = %v, want persist dynamic entity", err)
	}
	if _, ok := reg.Get("widget"); ok {
		t.Fatal("registry not rolled back after persist failure")
	}
	if ddl.guarded["ds_widget"] {
		t.Fatal("table guarded after persist failure")
	}
	if _, ok := loaded["widget"]; ok {
		t.Fatal("loaded recorded after persist failure")
	}
}

func TestDefineDynamicNilStoreUnchanged(t *testing.T) {
	reg, ddl := registry.New(), newFakeDDL()
	if err := defineDynamic(context.Background(), reg, ddl, nil, nil, spec()); err != nil {
		t.Fatalf("define: %v", err)
	}
	// Without a store there is no idempotent path: a second define is the
	// registry's duplicate error, exactly as before persistence existed.
	err := defineDynamic(context.Background(), reg, ddl, nil, nil, spec())
	if err == nil || !strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("err = %v, want registered twice", err)
	}
	if err := dropDynamic(context.Background(), reg, ddl, nil, nil, "widget"); err != nil {
		t.Fatalf("drop: %v", err)
	}
}

func TestDropDynamicDeletesPersistedBeforeTable(t *testing.T) {
	var ops []string
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(&ops), map[string]int64{}
	ddl.ops = &ops
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec()); err != nil {
		t.Fatalf("define: %v", err)
	}
	ops = ops[:0]
	if err := dropDynamic(context.Background(), reg, ddl, store, loaded, "widget"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if !reflect.DeepEqual(ops, []string{"delete:widget", "drop:ds_widget"}) {
		t.Fatalf("ops = %v, want [delete:widget drop:ds_widget]", ops)
	}
	if _, ok := loaded["widget"]; ok {
		t.Fatal("loaded still holds widget after drop")
	}
	if _, ok := store.rows["widget"]; ok {
		t.Fatal("row still persisted after drop")
	}
}

func TestDropDynamicResavesWhenTableDropFails(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec()); err != nil {
		t.Fatalf("define: %v", err)
	}
	ddl.failDrop = true
	if err := dropDynamic(context.Background(), reg, ddl, store, loaded, "widget"); err == nil {
		t.Fatal("expected the DDL error")
	}
	if _, ok := store.rows["widget"]; !ok {
		t.Fatal("previous spec not re-saved after the DDL drop failed")
	}
	if _, ok := reg.Get("widget"); !ok {
		t.Fatal("widget unregistered although its table survived")
	}
	if _, ok := loaded["widget"]; !ok {
		t.Fatal("loaded lost widget although it was re-saved")
	}
}

func TestAlterDynamicPersistsAlteredSpec(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec()); err != nil {
		t.Fatalf("define: %v", err)
	}
	altered := spec()
	altered.Schema.Columns = append(altered.Schema.Columns, registry.DynamicColumn{Name: "size", Type: registry.ColInt})
	if err := alterDynamic(context.Background(), reg, ddl, store, loaded, altered); err != nil {
		t.Fatalf("alter: %v", err)
	}
	got := store.lastSaved(t, "widget")
	if len(got.Schema.Columns) != 2 || got.Schema.Columns[1].Name != "size" {
		t.Fatalf("saved columns = %+v, want colour and size", got.Schema.Columns)
	}
	if loaded["widget"] != 2 {
		t.Fatalf("loaded = %v, want widget:2", loaded)
	}
}

func TestAlterDynamicSaveFailureKeepsNewSpec(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec()); err != nil {
		t.Fatalf("define: %v", err)
	}
	store.failSave = true
	altered := spec()
	altered.Schema.Columns = append(altered.Schema.Columns, registry.DynamicColumn{Name: "size", Type: registry.ColInt})
	err := alterDynamic(context.Background(), reg, ddl, store, loaded, altered)
	if err == nil || !strings.Contains(err.Error(), "persist dynamic entity") {
		t.Fatalf("err = %v, want persist dynamic entity", err)
	}
	ent, _ := reg.Get("widget")
	if ent == nil || !ent.Binding.HasColumn("size") {
		t.Fatal("altered spec not kept in the registry after a save failure")
	}
}

func TestRenameDynamicFieldPersistsRenamedSpec(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec()); err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := renameDynamicField(context.Background(), reg, ddl, store, loaded, "widget", "colour", "hue"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	got := store.lastSaved(t, "widget")
	if len(got.Schema.Columns) != 1 || got.Schema.Columns[0].Name != "hue" {
		t.Fatalf("saved columns = %+v, want hue only", got.Schema.Columns)
	}
}

func TestDropDynamicFieldPersistsSpec(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	s := spec()
	s.Schema.Columns = append(s.Schema.Columns, registry.DynamicColumn{Name: "size", Type: registry.ColInt})
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, s); err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := dropDynamicField(context.Background(), reg, ddl, store, loaded, "widget", "size"); err != nil {
		t.Fatalf("drop field: %v", err)
	}
	got := store.lastSaved(t, "widget")
	if len(got.Schema.Columns) != 1 || got.Schema.Columns[0].Name != "colour" {
		t.Fatalf("saved columns = %+v, want colour only", got.Schema.Columns)
	}
}

func fullSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: "orders", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_orders",
			Columns: []registry.DynamicColumn{
				{Name: "sku", Type: registry.ColText, NotNull: true, Default: "''"},
				{Name: "qty", Type: registry.ColInt},
				{Name: "price", Type: registry.ColFloat},
				{Name: "paid", Type: registry.ColBool},
				{Name: "at", Type: registry.ColTime, Default: "now()"},
				{Name: "meta", Type: registry.ColJSON},
				{Name: "site_id", Type: registry.ColText},
			},
			Indexes:     []registry.DynamicIndex{{Name: "ds_orders_sku_idx", Columns: []string{"sku", "qty"}, Unique: true}},
			NoTypeCheck: true,
		},
		GraphNode: "Order",
		Edges:     []registry.EdgeSpec{{Field: "site_id", Rel: "AT", Target: "site"}},
		Search:    registry.SearchSpec{Index: "orders", Fields: []string{"sku", "meta"}},
		Subscribe: []registry.Scope{registry.ByID, registry.ByField("site", "site_id")},
		Live:      &registry.LiveSpec{Filterable: []string{"sku"}, Sortable: []string{"at"}, MaxWindow: 50},
		Cache:     &registry.CacheSpec{TTL: 90 * time.Second, Scoped: true},
	}
}

func TestEncodeDecodeDynamicSpecRoundTrip(t *testing.T) {
	for _, in := range []registry.EntitySpec{fullSpec(), func() registry.EntitySpec {
		// GraphEdge is exclusive with GraphNode/Edges, so it gets its own case.
		s := fullSpec()
		s.GraphNode, s.Edges = "", nil
		s.GraphEdge = &registry.GraphEdgeSpec{
			TypeField: "sku", SourceField: "site_id", TargetField: "meta",
			SourceLabel: "Site", TargetLabel: "Thing", PropFields: []string{"qty", "price"},
		}
		return s
	}()} {
		b, err := encodeDynamicSpec(in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		out, err := decodeDynamicSpec(b)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !reflect.DeepEqual(out, in) {
			t.Fatalf("round trip mismatch:\n got  %+v\n want %+v", out, in)
		}
		// The registry must accept what comes back.
		if err := registry.New().Register(out); err != nil {
			t.Fatalf("register decoded: %v", err)
		}
	}
}

func TestEncodeDynamicSpecExactKeys(t *testing.T) {
	b, err := encodeDynamicSpec(spec())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	want := []string{"cache", "columns", "edges", "format", "graphEdge", "graphNode", "indexes",
		"live", "name", "noTypeCheck", "search", "subscribe", "table"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
	if string(m["format"]) != "1" || string(m["edges"]) != "[]" || string(m["live"]) != "null" {
		t.Fatalf("format/edges/live = %s/%s/%s", m["format"], m["edges"], m["live"])
	}
	if string(m["columns"]) != `[{"name":"colour","type":"text","notNull":false,"default":""}]` {
		t.Fatalf("columns = %s", m["columns"])
	}
}

func TestDecodeDynamicSpecRejectsUnknownFormat(t *testing.T) {
	_, err := decodeDynamicSpec([]byte(`{"format":2,"name":"widget","table":"ds_widget","columns":[]}`))
	if err == nil || !strings.Contains(err.Error(), "unsupported dynamic spec format 2") {
		t.Fatalf("err = %v, want unsupported dynamic spec format 2", err)
	}
}

func TestDecodeDynamicSpecRejectsUnknownColumnType(t *testing.T) {
	_, err := decodeDynamicSpec([]byte(`{"format":1,"name":"widget","table":"ds_widget","columns":[{"name":"c","type":"blob"}]}`))
	if err == nil || !strings.Contains(err.Error(), "blob") {
		t.Fatalf("err = %v, want unknown column type blob", err)
	}
}

func TestEncodeDynamicSpecDropsFuncFields(t *testing.T) {
	in := spec()
	in.Validate = func(map[string]any) error { return nil }
	in.Embed = &registry.EmbedSpec{Fields: []string{"colour"}}
	b, err := encodeDynamicSpec(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := decodeDynamicSpec(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Validate != nil || out.Embed != nil {
		t.Fatal("in-process fields survived the encoding")
	}
}

func TestSyncPersistedDynamic_AddsUpdatesRemoves(t *testing.T) {
	ctx := context.Background()
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.put(t, namedSpec("a"), 1)
	store.put(t, namedSpec("b"), 1)

	res, err := syncPersistedDynamic(ctx, reg, ddl, store, loaded)
	if err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	if !reflect.DeepEqual(res.Added, []string{"a", "b"}) || len(res.Updated)+len(res.Removed)+len(res.Skipped) != 0 {
		t.Fatalf("sync 1 = %+v, want Added [a b]", res)
	}
	if !ddl.guarded["ds_a"] || !ddl.guarded["ds_b"] {
		t.Fatalf("guarded = %v", ddl.guarded)
	}

	// Nothing changed: a second pass is a no-op.
	res, err = syncPersistedDynamic(ctx, reg, ddl, store, loaded)
	if err != nil || len(res.Added)+len(res.Updated)+len(res.Removed)+len(res.Skipped) != 0 {
		t.Fatalf("idle sync = %+v, %v, want empty", res, err)
	}

	b2 := namedSpec("b")
	b2.Schema.Columns = append(b2.Schema.Columns, registry.DynamicColumn{Name: "size", Type: registry.ColInt})
	store.put(t, b2, 2)
	res, err = syncPersistedDynamic(ctx, reg, ddl, store, loaded)
	if err != nil {
		t.Fatalf("sync 2: %v", err)
	}
	if !reflect.DeepEqual(res.Updated, []string{"b"}) || len(res.Added)+len(res.Removed) != 0 {
		t.Fatalf("sync 2 = %+v, want Updated [b]", res)
	}
	if ent, _ := reg.Get("b"); ent == nil || !ent.Binding.HasColumn("size") {
		t.Fatal("b not replaced with the new revision")
	}

	delete(store.rows, "a")
	res, err = syncPersistedDynamic(ctx, reg, ddl, store, loaded)
	if err != nil {
		t.Fatalf("sync 3: %v", err)
	}
	if !reflect.DeepEqual(res.Removed, []string{"a"}) || len(res.Added)+len(res.Updated) != 0 {
		t.Fatalf("sync 3 = %+v, want Removed [a]", res)
	}
	if _, ok := reg.Get("a"); ok {
		t.Fatal("a still registered")
	}
	if ddl.guarded["ds_a"] {
		t.Fatal("ds_a still guarded")
	}
	if len(ddl.dropped) != 0 {
		t.Fatalf("sync dropped tables: %v", ddl.dropped)
	}
	if _, ok := loaded["a"]; ok {
		t.Fatal("loaded still holds a")
	}
}

func TestSyncPersistedDynamic_SkipsNameRegisteredInCode(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	site := namedSpec("site")
	reg.MustRegister(site)
	persisted := namedSpec("site")
	persisted.Schema.Columns = append(persisted.Schema.Columns, registry.DynamicColumn{Name: "x", Type: registry.ColInt})
	store.put(t, persisted, 1)

	res, err := syncPersistedDynamic(context.Background(), reg, ddl, store, loaded)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if res.Skipped["site"] != "registered in code" {
		t.Fatalf("Skipped = %v, want site: registered in code", res.Skipped)
	}
	if ent, _ := reg.Get("site"); ent == nil || ent.Binding.HasColumn("x") {
		t.Fatal("the code-registered site was replaced")
	}
	if len(ddl.ensured) != 0 {
		t.Fatalf("ensured = %v, want none", ddl.ensured)
	}
	if _, ok := loaded["site"]; ok {
		t.Fatal("site recorded as loaded")
	}
}

func TestSyncPersistedDynamic_SkipsInvalidSpec(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.put(t, namedSpec("good"), 1)
	store.rows["bad"] = postgres.DynamicSpecRecord{Name: "bad", Spec: []byte("{not json"), Revision: 1}

	res, err := syncPersistedDynamic(context.Background(), reg, ddl, store, loaded)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !reflect.DeepEqual(res.Added, []string{"good"}) {
		t.Fatalf("Added = %v, want [good]", res.Added)
	}
	if !strings.HasPrefix(res.Skipped["bad"], "invalid spec:") {
		t.Fatalf("Skipped[bad] = %q, want invalid spec: ...", res.Skipped["bad"])
	}
	if _, ok := reg.Get("bad"); ok {
		t.Fatal("bad registered")
	}
}

func TestSyncPersistedDynamic_SkipsFutureFormat(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.rows["next"] = postgres.DynamicSpecRecord{Name: "next",
		Spec: []byte(`{"format":2,"name":"next","table":"ds_next","columns":[]}`), Revision: 1}
	res, err := syncPersistedDynamic(context.Background(), reg, ddl, store, loaded)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !strings.Contains(res.Skipped["next"], "unsupported dynamic spec format 2") {
		t.Fatalf("Skipped = %v", res.Skipped)
	}
}

func TestSyncPersistedDynamic_SkipsWhenEnsureFails(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.put(t, namedSpec("a"), 1)
	ddl.failEnsure = true
	res, err := syncPersistedDynamic(context.Background(), reg, ddl, store, loaded)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !strings.HasPrefix(res.Skipped["a"], "ensure table:") {
		t.Fatalf("Skipped = %v, want a: ensure table: ...", res.Skipped)
	}
	if _, ok := reg.Get("a"); ok {
		t.Fatal("a registered without a table")
	}
	// The next pass retries once the DDL works.
	ddl.failEnsure = false
	res, err = syncPersistedDynamic(context.Background(), reg, ddl, store, loaded)
	if err != nil || !reflect.DeepEqual(res.Added, []string{"a"}) {
		t.Fatalf("retry = %+v, %v, want Added [a]", res, err)
	}
}

func TestSyncPersistedDynamic_UpdateKeepsInProcessFields(t *testing.T) {
	ctx := context.Background()
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	withValidate := spec()
	withValidate.Validate = func(map[string]any) error { return nil }
	if err := defineDynamic(ctx, reg, ddl, store, loaded, withValidate); err != nil {
		t.Fatalf("define: %v", err)
	}
	// Another instance adds a column.
	next := spec()
	next.Schema.Columns = append(next.Schema.Columns, registry.DynamicColumn{Name: "size", Type: registry.ColInt})
	store.put(t, next, 2)
	if _, err := syncPersistedDynamic(ctx, reg, ddl, store, loaded); err != nil {
		t.Fatalf("sync: %v", err)
	}
	ent, _ := reg.Get("widget")
	if ent == nil || !ent.Binding.HasColumn("size") || ent.Spec.Validate == nil {
		t.Fatal("update lost the new column or the in-process Validate")
	}
}

func TestConfigValidate_DynamicPersistRejectsCatalogMode(t *testing.T) {
	cfg := Config{
		Catalog: CatalogConfig{DSN: "postgres://control", ClusterDSNs: map[string]string{"c1": "postgres://c1"}},
		Dynamic: DynamicConfig{Persist: true},
	}
	err := cfg.Validate()
	if err == nil || err.Error() != "fabriq: dynamic.persist is static-mode only" {
		t.Fatalf("err = %v, want static-mode only", err)
	}
	cfg.Dynamic.Persist = false
	if err := cfg.Validate(); err != nil {
		t.Fatalf("catalog without persist: %v", err)
	}
	static := Config{Postgres: PostgresConfig{DSN: "postgres://x"}, Dynamic: DynamicConfig{Persist: true}}
	if err := static.Validate(); err != nil {
		t.Fatalf("static with persist: %v", err)
	}
}

func TestSyncPersistedDynamic_UpdatesWhenRedefinedAtSameRevision(t *testing.T) {
	ctx := context.Background()
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.put(t, spec(), 1)
	if _, err := syncPersistedDynamic(ctx, reg, ddl, store, loaded); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	// Another instance drops and redefines widget between two passes: the
	// row restarts at revision 1 with a different spec.
	redefined := spec()
	redefined.Schema.Columns = append(redefined.Schema.Columns, registry.DynamicColumn{Name: "size", Type: registry.ColInt})
	store.put(t, redefined, 1)
	res, err := syncPersistedDynamic(ctx, reg, ddl, store, loaded)
	if err != nil {
		t.Fatalf("sync 2: %v", err)
	}
	if !reflect.DeepEqual(res.Updated, []string{"widget"}) {
		t.Fatalf("sync 2 = %+v, want Updated [widget]", res)
	}
	if ent, _ := reg.Get("widget"); ent == nil || !ent.Binding.HasColumn("size") {
		t.Fatal("widget not replaced with the redefined spec")
	}
}

func TestSyncPersistedDynamic_UpdatesWhenRevisionGoesDown(t *testing.T) {
	ctx := context.Background()
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.put(t, spec(), 3)
	if _, err := syncPersistedDynamic(ctx, reg, ddl, store, loaded); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	redefined := spec()
	redefined.Schema.Columns = append(redefined.Schema.Columns, registry.DynamicColumn{Name: "size", Type: registry.ColInt})
	store.put(t, redefined, 1)
	res, err := syncPersistedDynamic(ctx, reg, ddl, store, loaded)
	if err != nil {
		t.Fatalf("sync 2: %v", err)
	}
	if !reflect.DeepEqual(res.Updated, []string{"widget"}) || loaded["widget"] != 1 {
		t.Fatalf("sync 2 = %+v loaded = %v, want Updated [widget] at revision 1", res, loaded)
	}
}

func TestDefineDynamicAdoptsEqualSpecPersistedElsewhere(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.put(t, spec(), 1) // persisted by another instance; this one has not synced
	withValidate := spec()
	withValidate.Validate = func(map[string]any) error { return nil }
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, withValidate); err != nil {
		t.Fatalf("define: %v", err)
	}
	if store.saves != 0 {
		t.Fatalf("saves = %d, want 0", store.saves)
	}
	ent, ok := reg.Get("widget")
	if !ok || ent.Spec.Validate == nil {
		t.Fatal("widget not registered with the caller's Validate")
	}
	if loaded["widget"] != 1 {
		t.Fatalf("loaded = %v, want widget:1", loaded)
	}
}

func TestDefineDynamicRefusesDifferentSpecPersistedElsewhere(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.put(t, spec(), 1)
	before := string(store.rows["widget"].Spec)
	other := spec()
	other.Schema.Columns[0].NotNull = true // tightening a column is not additive
	err := defineDynamic(context.Background(), reg, ddl, store, loaded, other)
	if err == nil || !strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("err = %v, want registered twice", err)
	}
	if store.saves != 0 || string(store.rows["widget"].Spec) != before || store.rows["widget"].Revision != 1 {
		t.Fatalf("stored row changed: saves=%d row=%+v", store.saves, store.rows["widget"])
	}
}

func TestDefineDynamicRefusesUnloadablePersistedRow(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	future := []byte(`{"format":2,"name":"widget","table":"ds_widget","columns":[]}`)
	store.rows["widget"] = postgres.DynamicSpecRecord{Name: "widget", Spec: future, Revision: 4}
	err := defineDynamic(context.Background(), reg, ddl, store, loaded, spec())
	if err == nil || !strings.Contains(err.Error(), "cannot load") {
		t.Fatalf("err = %v, want cannot load", err)
	}
	if store.saves != 0 || string(store.rows["widget"].Spec) != string(future) {
		t.Fatal("unloadable row overwritten")
	}
	if _, ok := reg.Get("widget"); ok {
		t.Fatal("widget registered")
	}
}

func TestDropDynamicLeavesRowOfTypeRegisteredInCode(t *testing.T) {
	var ops []string
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(&ops), map[string]int64{}
	ddl.ops = &ops
	reg.MustRegister(namedSpec("site"))
	store.put(t, namedSpec("site"), 1)
	if err := dropDynamic(context.Background(), reg, ddl, store, loaded, "site"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, ok := store.rows["site"]; !ok {
		t.Fatal("another instance's site row was deleted")
	}
	for _, op := range ops {
		if op == "delete:site" {
			t.Fatalf("ops = %v, want no delete:site", ops)
		}
	}
}

// additiveSpec is spec() plus a new size column and a new index on it.
func additiveSpec() registry.EntitySpec {
	sp := spec()
	sp.Schema.Columns = append(sp.Schema.Columns, registry.DynamicColumn{Name: "size", Type: registry.ColInt})
	sp.Schema.Indexes = append(sp.Schema.Indexes, registry.DynamicIndex{Name: "ds_widget_size_idx", Columns: []string{"size"}})
	return sp
}

// TestDefineDynamicAdditiveChangeAlters: a library app that adds a column to
// its boot-time define, over a type the Open-time sync loaded, gets the alter
// path (DDL, registry swap with its in-process fields, save) and no
// "registered twice".
func TestDefineDynamicAdditiveChangeAlters(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	store.put(t, spec(), 1)
	if _, err := syncPersistedDynamic(context.Background(), reg, ddl, store, loaded); err != nil {
		t.Fatalf("sync: %v", err)
	}
	next := additiveSpec()
	next.Validate = func(map[string]any) error { return nil }
	if err := defineDynamic(context.Background(), reg, ddl, store, loaded, next); err != nil {
		t.Fatalf("additive define: %v", err)
	}
	if store.saves != 1 || store.rows["widget"].Revision != 2 {
		t.Fatalf("saves = %d row = %+v, want one save at revision 2", store.saves, store.rows["widget"])
	}
	got := store.lastSaved(t, "widget")
	if len(got.Schema.Columns) != 2 || len(got.Schema.Indexes) != 1 {
		t.Fatalf("saved schema = %+v, want colour+size and one index", got.Schema)
	}
	if loaded["widget"] != 2 {
		t.Fatalf("loaded = %v, want widget:2", loaded)
	}
	if len(ddl.ensured) != 2 {
		t.Fatalf("ensured = %v, want the sync's and the alter's EnsureDynamic", ddl.ensured)
	}
	ent, ok := reg.Get("widget")
	if !ok || !ent.Binding.HasColumn("size") || ent.Spec.Validate == nil {
		t.Fatal("registry does not hold the altered spec with the caller's Validate")
	}

	// The next sync sees its own revision and leaves the type alone.
	res, err := syncPersistedDynamic(context.Background(), reg, ddl, store, loaded)
	if err != nil {
		t.Fatalf("sync after alter: %v", err)
	}
	if len(res.Updated) != 0 {
		t.Fatalf("sync after alter updated %v, want nothing", res.Updated)
	}
}

// TestDefineDynamicNonAdditiveChangeStillRegisteredTwice: anything other than
// new columns and indexes keeps failing, and the persisted row stays as it was.
func TestDefineDynamicNonAdditiveChangeStillRegisteredTwice(t *testing.T) {
	cases := map[string]func(*registry.EntitySpec){
		"column type change": func(sp *registry.EntitySpec) { sp.Schema.Columns[0].Type = registry.ColInt },
		"not null change":    func(sp *registry.EntitySpec) { sp.Schema.Columns[0].NotNull = true },
		"default change":     func(sp *registry.EntitySpec) { sp.Schema.Columns[0].Default = "'red'" },
		"dropped column": func(sp *registry.EntitySpec) {
			sp.Schema.Columns = []registry.DynamicColumn{{Name: "size", Type: registry.ColInt}}
			sp.Schema.Indexes = nil
		},
		"dropped index":   func(sp *registry.EntitySpec) { sp.Schema.Indexes = nil },
		"different table": func(sp *registry.EntitySpec) { sp.Schema.Table = "ds_widget2" },
		"changed index": func(sp *registry.EntitySpec) {
			sp.Schema.Indexes = []registry.DynamicIndex{{Name: "ds_widget_colour_idx", Columns: []string{"colour"}, Unique: true}}
		},
		"graph node change": func(sp *registry.EntitySpec) { sp.GraphNode = "Gizmo" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
			base := spec()
			base.Schema.Indexes = []registry.DynamicIndex{{Name: "ds_widget_colour_idx", Columns: []string{"colour"}}}
			store.put(t, base, 1)
			if _, err := syncPersistedDynamic(context.Background(), reg, ddl, store, loaded); err != nil {
				t.Fatalf("sync: %v", err)
			}
			before := string(store.rows["widget"].Spec)
			next := base
			sc := *base.Schema
			sc.Columns = append([]registry.DynamicColumn(nil), base.Schema.Columns...)
			sc.Indexes = append([]registry.DynamicIndex(nil), base.Schema.Indexes...)
			next.Schema = &sc
			mutate(&next)
			err := defineDynamic(context.Background(), reg, ddl, store, loaded, next)
			if err == nil || !strings.Contains(err.Error(), "registered twice") {
				t.Fatalf("err = %v, want registered twice", err)
			}
			if store.saves != 0 || string(store.rows["widget"].Spec) != before {
				t.Fatalf("row changed: saves=%d row=%s", store.saves, store.rows["widget"].Spec)
			}
			ent, _ := reg.Get("widget")
			if ent == nil || !sameDynamicEncoding(ent.Spec, base) {
				t.Fatal("registry no longer holds the loaded spec")
			}
		})
	}
}

// TestDefineDynamicAdditiveChangeOfCodeTypeStillRegisteredTwice: a type
// registered in code is not this facade's to alter through a define.
func TestDefineDynamicAdditiveChangeOfCodeTypeStillRegisteredTwice(t *testing.T) {
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	reg.MustRegister(spec())
	err := defineDynamic(context.Background(), reg, ddl, store, loaded, additiveSpec())
	if err == nil || !strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("err = %v, want registered twice", err)
	}
	if store.saves != 0 {
		t.Fatalf("saves = %d, want 0", store.saves)
	}
}

// TestSchemaOpsOnCodeTypeWriteNoRow: alter, rename-field and drop-field on a
// dynamic type registered in code run their DDL and update the registry, but
// never write a row to the shared table (another instance may own that name).
func TestSchemaOpsOnCodeTypeWriteNoRow(t *testing.T) {
	ctx := context.Background()
	reg, ddl, store, loaded := registry.New(), newFakeDDL(), newFakeSpecStore(nil), map[string]int64{}
	base := spec()
	base.Schema.Columns = append(base.Schema.Columns, registry.DynamicColumn{Name: "shape", Type: registry.ColText})
	reg.MustRegister(base)

	altered := spec()
	altered.Schema.Columns = append(altered.Schema.Columns,
		registry.DynamicColumn{Name: "shape", Type: registry.ColText},
		registry.DynamicColumn{Name: "size", Type: registry.ColInt})
	if err := alterDynamic(ctx, reg, ddl, store, loaded, altered); err != nil {
		t.Fatalf("alter: %v", err)
	}
	if err := renameDynamicField(ctx, reg, ddl, store, loaded, "widget", "colour", "hue"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := dropDynamicField(ctx, reg, ddl, store, loaded, "widget", "shape"); err != nil {
		t.Fatalf("drop field: %v", err)
	}
	if store.saves != 0 || len(store.rows) != 0 {
		t.Fatalf("saves = %d rows = %v, want no row written", store.saves, store.rows)
	}
	if _, ok := loaded["widget"]; ok {
		t.Fatal("code-registered widget recorded as loaded")
	}
	ent, _ := reg.Get("widget")
	if ent == nil || !ent.Binding.HasColumn("hue") || !ent.Binding.HasColumn("size") || ent.Binding.HasColumn("shape") {
		t.Fatal("registry does not reflect the alter, rename and drop")
	}
	if len(ddl.renamed) != 1 || len(ddl.droppedCols) != 1 {
		t.Fatalf("ddl renamed=%v droppedCols=%v, want one each", ddl.renamed, ddl.droppedCols)
	}
}
