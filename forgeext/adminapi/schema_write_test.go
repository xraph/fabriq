package adminapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
)

func TestKindToColumnType(t *testing.T) {
	cases := map[string]registry.ColumnType{
		"string": registry.ColText, "number": registry.ColFloat,
		"boolean": registry.ColBool, "time": registry.ColTime, "object": registry.ColJSON,
	}
	for kind, want := range cases {
		got, err := kindToColumnType(kind)
		if err != nil || got != want {
			t.Fatalf("kind %q: got (%v,%v) want %v", kind, got, err, want)
		}
	}
	if _, err := kindToColumnType("bogus"); err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

func TestValidateDefaultExpr(t *testing.T) {
	ok := []string{"", "42", "-3.14", "true", "FALSE", "null", "now()", "NOW()", "'pending'", "''"}
	for _, s := range ok {
		if err := validateDefaultExpr(s); err != nil {
			t.Fatalf("expected %q allowed, got %v", s, err)
		}
	}
	bad := []string{"nextval('x')", "'a''b'", "1;DROP TABLE t", "now() + 1", "gen_random_uuid()", "'x'||'y'"}
	for _, s := range bad {
		if err := validateDefaultExpr(s); err == nil {
			t.Fatalf("expected %q rejected", s)
		}
	}
}

func TestValidSchemaIdent(t *testing.T) {
	if !validSchemaIdent("order") || !validSchemaIdent("order_line2") {
		t.Fatal("valid idents rejected")
	}
	for _, s := range []string{"", "2bad", "a-b", "drop table", "a;b"} {
		if validSchemaIdent(s) {
			t.Fatalf("invalid ident %q accepted", s)
		}
	}
}

func TestTableFor(t *testing.T) {
	if tableFor("order") != "ds_order" {
		t.Fatalf("tableFor(order) = %q", tableFor("order"))
	}
}

// --- handler tests: fake writer, no Postgres ---

type fakeWriter struct {
	defined  []registry.EntitySpec
	altered  []registry.EntitySpec
	renamed  [][3]string // type, from, to
	dropped  []string
	droppedF [][2]string // type, col
	failWith error
}

func (f *fakeWriter) DefineDynamic(_ context.Context, s registry.EntitySpec) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.defined = append(f.defined, s)
	return nil
}

func (f *fakeWriter) AlterDynamic(_ context.Context, s registry.EntitySpec) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.altered = append(f.altered, s)
	return nil
}

func (f *fakeWriter) RenameDynamicField(_ context.Context, t, a, b string) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.renamed = append(f.renamed, [3]string{t, a, b})
	return nil
}

func (f *fakeWriter) DropDynamicField(_ context.Context, t, c string) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.droppedF = append(f.droppedF, [2]string{t, c})
	return nil
}

func (f *fakeWriter) DropDynamic(_ context.Context, t string) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.dropped = append(f.dropped, t)
	return nil
}

// writerBackedExt builds an Extension whose dynamic-schema writer is the given
// fake, bypassing Start / fabriq.Open. The tenant middleware is attached so all
// routes require the X-Tenant-ID header, matching the other handler harnesses.
func writerBackedExt(t *testing.T, w dynamicSchemaWriter) *Extension {
	t.Helper()
	e := NewAdminAPI(nil, WithRouteOptions(forge.WithMiddleware(tenantMiddleware)))
	e.dynWriter = w
	e.reg = registry.New() // handlers don't read the registry except handleAddFields
	return e
}

func doJSON(t *testing.T, method, url string, body any) *http.Response {
	t.Helper()
	var r *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		r = bytes.NewReader(b)
	} else {
		r = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(testTenantHeader, testTenantID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

func TestHandleDefineDynamic(t *testing.T) {
	w := &fakeWriter{}
	srv := buildServer(t, writerBackedExt(t, w))
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema", map[string]any{
		"type":    "gadget",
		"columns": []map[string]any{{"name": "label", "kind": "string", "required": true}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(w.defined) != 1 {
		t.Fatalf("defined len = %d, want 1", len(w.defined))
	}
	spec := w.defined[0]
	if spec.Name != "gadget" {
		t.Errorf("spec.Name = %q, want %q", spec.Name, "gadget")
	}
	if spec.Kind != registry.KindAggregate {
		t.Errorf("spec.Kind = %v, want KindAggregate", spec.Kind)
	}
	if spec.Schema == nil || spec.Schema.Table != "ds_gadget" {
		t.Fatalf("spec.Schema.Table = %+v, want ds_gadget", spec.Schema)
	}
	if len(spec.Schema.Columns) != 1 {
		t.Fatalf("columns len = %d, want 1", len(spec.Schema.Columns))
	}
	col := spec.Schema.Columns[0]
	if col.Name != "label" || col.Type != registry.ColText || !col.NotNull {
		t.Errorf("column = %+v, want {label ColText NotNull=true}", col)
	}
}

func TestHandleDefineRejectsBadKind(t *testing.T) {
	w := &fakeWriter{}
	srv := buildServer(t, writerBackedExt(t, w))
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema", map[string]any{
		"type":    "gadget",
		"columns": []map[string]any{{"name": "label", "kind": "bogus"}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if len(w.defined) != 0 {
		t.Fatalf("defined len = %d, want 0", len(w.defined))
	}
}

func TestHandleDefineRejectsBadDefault(t *testing.T) {
	w := &fakeWriter{}
	srv := buildServer(t, writerBackedExt(t, w))
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema", map[string]any{
		"type":    "gadget",
		"columns": []map[string]any{{"name": "label", "kind": "string", "default": "nextval('x')"}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if len(w.defined) != 0 {
		t.Fatalf("defined len = %d, want 0", len(w.defined))
	}
}

// TestHandleAddFields verifies the happy path: AlterDynamic is called with the
// type name and the new columns when the type is not already registered
// (union has nothing to union with beyond the new set).
func TestHandleAddFields(t *testing.T) {
	w := &fakeWriter{}
	ext := writerBackedExt(t, w)
	if err := ext.reg.Register(registry.EntitySpec{
		Name: "gadget", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table:   "ds_gadget",
			Columns: []registry.DynamicColumn{{Name: "label", Type: registry.ColText, NotNull: true}},
		},
	}); err != nil {
		t.Fatalf("register gadget: %v", err)
	}
	srv := buildServer(t, ext)
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema/gadget/fields", map[string]any{
		"columns": []map[string]any{{"name": "weight", "kind": "number"}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(w.altered) != 1 {
		t.Fatalf("altered len = %d, want 1", len(w.altered))
	}
	spec := w.altered[0]
	if spec.Name != "gadget" {
		t.Errorf("spec.Name = %q, want %q", spec.Name, "gadget")
	}
}

// TestHandleAddFields_UnionsExistingColumns is the critical descriptor
// preservation test: AlterDynamic must receive the FULL column set (existing +
// new), not just the new columns, or the registry descriptor would lose the
// pre-existing columns.
func TestHandleAddFields_UnionsExistingColumns(t *testing.T) {
	w := &fakeWriter{}
	ext := writerBackedExt(t, w)
	if err := ext.reg.Register(registry.EntitySpec{
		Name: "gadget", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_gadget",
			Columns: []registry.DynamicColumn{
				{Name: "label", Type: registry.ColText, NotNull: true},
				{Name: "colour", Type: registry.ColText},
			},
		},
	}); err != nil {
		t.Fatalf("register gadget: %v", err)
	}
	srv := buildServer(t, ext)
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema/gadget/fields", map[string]any{
		"columns": []map[string]any{{"name": "weight", "kind": "number"}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(w.altered) != 1 {
		t.Fatalf("altered len = %d, want 1", len(w.altered))
	}
	byName := map[string]registry.DynamicColumn{}
	for _, c := range w.altered[0].Schema.Columns {
		byName[c.Name] = c
	}
	for _, want := range []string{"label", "colour", "weight"} {
		if _, ok := byName[want]; !ok {
			t.Errorf("altered spec missing column %q; got %+v", want, w.altered[0].Schema.Columns)
		}
	}
	if len(byName) != 3 {
		t.Errorf("altered spec column count = %d, want 3 (got %+v)", len(byName), w.altered[0].Schema.Columns)
	}
}

// TestHandleAddFields_NewColumnOverridesOnConflict verifies that when the new
// column set redefines an existing column name, the new definition wins.
func TestHandleAddFields_NewColumnOverridesOnConflict(t *testing.T) {
	w := &fakeWriter{}
	ext := writerBackedExt(t, w)
	if err := ext.reg.Register(registry.EntitySpec{
		Name: "gadget", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table:   "ds_gadget",
			Columns: []registry.DynamicColumn{{Name: "label", Type: registry.ColText}},
		},
	}); err != nil {
		t.Fatalf("register gadget: %v", err)
	}
	srv := buildServer(t, ext)
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema/gadget/fields", map[string]any{
		"columns": []map[string]any{{"name": "label", "kind": "number", "required": true}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(w.altered) != 1 || len(w.altered[0].Schema.Columns) != 1 {
		t.Fatalf("altered = %+v, want exactly 1 column", w.altered)
	}
	col := w.altered[0].Schema.Columns[0]
	if col.Name != "label" || col.Type != registry.ColFloat || !col.NotNull {
		t.Errorf("column = %+v, want overridden {label ColFloat NotNull=true}", col)
	}
}

func TestHandleAddFields_UnknownType(t *testing.T) {
	w := &fakeWriter{}
	srv := buildServer(t, writerBackedExt(t, w))
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema/nosuchtype/fields", map[string]any{
		"columns": []map[string]any{{"name": "weight", "kind": "number"}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if len(w.altered) != 0 {
		t.Fatalf("altered len = %d, want 0", len(w.altered))
	}
}

func TestHandleRenameField(t *testing.T) {
	w := &fakeWriter{}
	srv := buildServer(t, writerBackedExt(t, w))
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema/gadget/rename-field", map[string]any{
		"from": "a", "to": "b",
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	want := [3]string{"gadget", "a", "b"}
	if len(w.renamed) != 1 || w.renamed[0] != want {
		t.Fatalf("renamed = %v, want [%v]", w.renamed, want)
	}
}

func TestHandleDropFieldRequiresConfirm(t *testing.T) {
	w := &fakeWriter{}
	srv := buildServer(t, writerBackedExt(t, w))
	defer srv.Close()

	resp := doJSON(t, http.MethodDelete, srv.URL+"/admin/schema/gadget/fields/label", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if len(w.droppedF) != 0 {
		t.Fatalf("droppedF len = %d, want 0", len(w.droppedF))
	}

	resp2 := doJSON(t, http.MethodDelete, srv.URL+"/admin/schema/gadget/fields/label?confirm=label", nil)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp2.StatusCode)
	}
	want := [2]string{"gadget", "label"}
	if len(w.droppedF) != 1 || w.droppedF[0] != want {
		t.Fatalf("droppedF = %v, want [%v]", w.droppedF, want)
	}
}

func TestHandleDropTypeRequiresConfirm(t *testing.T) {
	w := &fakeWriter{}
	srv := buildServer(t, writerBackedExt(t, w))
	defer srv.Close()

	resp := doJSON(t, http.MethodDelete, srv.URL+"/admin/schema/gadget", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}

	resp2 := doJSON(t, http.MethodDelete, srv.URL+"/admin/schema/gadget?confirm=gadget", nil)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp2.StatusCode)
	}
	if len(w.dropped) != 1 || w.dropped[0] != "gadget" {
		t.Fatalf("dropped = %v, want [gadget]", w.dropped)
	}
}

func TestHandleDefineUnavailable(t *testing.T) {
	w := &fakeWriter{failWith: fabriq.ErrDynamicUnavailable}
	srv := buildServer(t, writerBackedExt(t, w))
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema", map[string]any{
		"type":    "gadget",
		"columns": []map[string]any{{"name": "label", "kind": "string"}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}

func TestHandleAlterUnknownType(t *testing.T) {
	// The registry has the type registered (so the handler's own union lookup
	// passes), but the facade itself reports "unknown" (e.g. a race where the
	// type was dropped between the registry read and the DDL call).
	w := &fakeWriter{failWith: errUnknownEntityForTest("gadget")}
	ext := writerBackedExt(t, w)
	if err := ext.reg.Register(registry.EntitySpec{
		Name: "gadget", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table:   "ds_gadget",
			Columns: []registry.DynamicColumn{{Name: "label", Type: registry.ColText}},
		},
	}); err != nil {
		t.Fatalf("register gadget: %v", err)
	}
	srv := buildServer(t, ext)
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema/gadget/fields", map[string]any{
		"columns": []map[string]any{{"name": "weight", "kind": "number"}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// errUnknownEntityForTest mimics the SP1 facade's "cannot alter unknown
// entity" message shape so mapSchemaError's string-matching path is exercised.
type errUnknownEntityForTest string

func (e errUnknownEntityForTest) Error() string {
	return "fabriq: cannot alter unknown entity \"" + string(e) + "\""
}

func TestHandleDefineDuplicate(t *testing.T) {
	w := &fakeWriter{failWith: fmt.Errorf("fabriq: entity %q registered twice", "gadget")}
	srv := buildServer(t, writerBackedExt(t, w))
	defer srv.Close()

	resp := doJSON(t, http.MethodPost, srv.URL+"/admin/schema", map[string]any{
		"type":    "gadget",
		"columns": []map[string]any{{"name": "label", "kind": "string", "required": true}},
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
}
