package elastic

import (
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/fabriqerr"
)

func TestTranslateESResponse(t *testing.T) {
	body := `{"error":{"type":"index_not_found_exception","reason":"no such index [x]"},"status":404}`
	out := translateESResponse("search x", 404, body)
	var fe *fabriqerr.Error
	if !errors.As(out, &fe) || fe.Code != fabriqerr.CodeNotFound {
		t.Fatalf("404 → not_found, got %T %v", out, out)
	}
	if fe.Meta.Driver != "elastic" || fe.Meta.Detail["type"] != "index_not_found_exception" ||
		fe.Meta.Detail["status"] != "404" || fe.Meta.Detail["reason"] != "no such index [x]" {
		t.Fatalf("meta not parsed: %+v", fe.Meta)
	}
	// Full raw body must not appear in the caller-facing message.
	if fe.Message == body || esContains(fe.Error(), "no such index") {
		t.Fatalf("raw body/reason must not be in the message: %q", fe.Error())
	}
}

func TestTranslateESResponse_StatusMap(t *testing.T) {
	cases := map[int]fabriqerr.Code{
		400: fabriqerr.CodeInvalidInput,
		401: fabriqerr.CodeUnauthorized,
		403: fabriqerr.CodePermissionDenied,
		404: fabriqerr.CodeNotFound,
		409: fabriqerr.CodeAlreadyExists,
		429: fabriqerr.CodeUnavailable,
		503: fabriqerr.CodeUnavailable,
		418: fabriqerr.CodeInternal,
	}
	for status, want := range cases {
		out := translateESResponse("op", status, `{}`)
		var fe *fabriqerr.Error
		if !errors.As(out, &fe) || fe.Code != want {
			t.Errorf("status %d → got %v, want %q", status, out, want)
		}
	}
}

func TestTranslateESResponse_VersionConflictType(t *testing.T) {
	body := `{"error":{"type":"version_conflict_engine_exception","reason":"..."}}`
	out := translateESResponse("bulk", 409, body)
	var fe *fabriqerr.Error
	if !errors.As(out, &fe) || fe.Code != fabriqerr.CodeVersionConflict {
		t.Fatalf("version_conflict type → version_conflict code, got %v", out)
	}
}

func TestTranslateES_Transport(t *testing.T) {
	if translateES("op", nil) != nil {
		t.Fatal("nil stays nil")
	}
	out := translateES("search x", errors.New("dial tcp: connection refused"))
	var fe *fabriqerr.Error
	if !errors.As(out, &fe) || fe.Code != fabriqerr.CodeUnavailable {
		t.Fatalf("transport conn error → unavailable, got %v", out)
	}
	if fe.Meta.Detail["driverMessage"] != "dial tcp: connection refused" {
		t.Fatalf("raw transport message must be in Meta.Detail, got %+v", fe.Meta.Detail)
	}
}

func TestTranslateES_PassThrough(t *testing.T) {
	existing := fabriqerr.New(fabriqerr.CodeNotFound, "x")
	if got := translateES("op", existing); got != error(existing) {
		t.Fatal("structured error must pass through unchanged")
	}
}

func esContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
