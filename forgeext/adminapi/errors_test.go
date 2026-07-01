package adminapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// renderErrorProbe is a test-only forge.Controller that exposes renderError
// over a real HTTP route so tests can assert the rendered status/body through
// the same server/router harness the rest of the adminapi test suite uses
// (buildServer + a real http.Client), rather than hand-constructing a
// forge.Context. Each route renders a fixed error selected by the "which"
// path param.
type renderErrorProbe struct{}

func (renderErrorProbe) Name() string { return "test:render-error-probe" }

func (renderErrorProbe) Routes(r forge.Router) error {
	return r.GET("/probe/render-error/:which", func(ctx forge.Context) error {
		switch ctx.Param("which") {
		case "schema-mismatch":
			cause := fmt.Errorf(`pgdriver: tx query: ERROR: relation "products" does not exist (SQLSTATE 42P01)`)
			fe := fabriqerr.Wrap(fabriqerr.CodeSchemaMismatch, cause,
				fabriqerr.SafeMessage(fabriqerr.CodeSchemaMismatch),
				fabriqerr.WithEntity("products", ""), fabriqerr.WithOp("list"),
				fabriqerr.WithMeta(fabriqerr.Meta{
					Driver: "postgres", SQLState: "42P01", Table: "products",
					Detail: map[string]string{"driverMessage": `relation "products" does not exist`},
				}))
			return renderError(ctx, fe)
		case "unstructured":
			return renderError(ctx, fmt.Errorf(`pgdriver: tx query: ERROR: boom (SQLSTATE XX000)`))
		case "not-found":
			return renderError(ctx, &fabriqerr.NotFoundError{Entity: "site", ID: "1"})
		case "version-conflict":
			return renderError(ctx, &fabriqerr.VersionConflictError{Aggregate: "a", AggID: "1", Expected: 1, Actual: 2})
		default:
			return forge.BadRequest("unknown probe: " + ctx.Param("which"))
		}
	})
}

var _ forge.Controller = renderErrorProbe{}

// buildRenderErrorProbeServer registers the render-error probe controller on a
// fresh forge app and returns a test HTTP server, mirroring buildServer's
// pattern for the rest of the adminapi handler tests.
func buildRenderErrorProbeServer(t *testing.T) *httptest.Server {
	t.Helper()
	app := forge.NewApp(forge.AppConfig{Name: "render-error-probe-test", HTTPAddress: ":0"})
	if err := app.RegisterController(renderErrorProbe{}); err != nil {
		t.Fatalf("register controller: %v", err)
	}
	return httptest.NewServer(app.Router().Handler())
}

func probeGet(t *testing.T, srv *httptest.Server, which string) *http.Response {
	t.Helper()
	resp, err := http.Get(srv.URL + "/probe/render-error/" + which)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func decodeErrorBody(t *testing.T, resp *http.Response) errorBody {
	t.Helper()
	defer resp.Body.Close()
	var b errorBody
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		t.Fatalf("body not valid errorBody JSON: %v", err)
	}
	return b
}

func TestRenderError_StructuredSchemaMismatch(t *testing.T) {
	srv := buildRenderErrorProbeServer(t)
	defer srv.Close()

	resp := probeGet(t, srv, "schema-mismatch")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	body := decodeErrorBody(t, resp)
	if body.Error.Code != "schema_mismatch" || body.Error.Entity != "products" {
		t.Fatalf("payload wrong: %+v", body.Error)
	}
	if body.Error.Meta.SQLState != "42P01" {
		t.Fatalf("meta not rendered: %+v", body.Error.Meta)
	}
	// The raw driver blob must NOT appear at the top level (only under meta.detail).
	if strings.Contains(body.Error.Message, "pgdriver") ||
		strings.Contains(body.Error.Message, "SQLSTATE") {
		t.Fatalf("driver text leaked into message: %q", body.Error.Message)
	}
}

func TestRenderError_UnstructuredIsGeneric500(t *testing.T) {
	srv := buildRenderErrorProbeServer(t)
	defer srv.Close()

	resp := probeGet(t, srv, "unstructured")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	body := decodeErrorBody(t, resp)
	if body.Error.Code != "internal" {
		t.Fatalf("code = %q, want internal", body.Error.Code)
	}
	if strings.Contains(body.Error.Message, "pgdriver") {
		t.Fatalf("raw driver text leaked in 500 body message: %q", body.Error.Message)
	}
}

func TestRenderError_LegacyRichTypes(t *testing.T) {
	srv := buildRenderErrorProbeServer(t)
	defer srv.Close()

	resp := probeGet(t, srv, "not-found")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("NotFoundError status = %d, want 404", resp.StatusCode)
	}

	resp2 := probeGet(t, srv, "version-conflict")
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("VersionConflictError status = %d, want 409", resp2.StatusCode)
	}
}
