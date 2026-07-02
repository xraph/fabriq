package trovestore

import (
	"errors"
	"testing"

	"github.com/xraph/trove"

	"github.com/xraph/fabriq/core/fabriqerr"
)

func TestMapErr_NotFoundIsStructured(t *testing.T) {
	out := mapErr(trove.ErrNotFound)
	var fe *fabriqerr.Error
	if !errors.As(out, &fe) || fe.Code != fabriqerr.CodeNotFound {
		t.Fatalf("want structured CodeNotFound, got %T %v", out, out)
	}
	if !errors.Is(out, fabriqerr.ErrNotFound) {
		t.Fatal("must still satisfy errors.Is(err, ErrNotFound) for backward compat")
	}
	if fe.Meta.Driver != "trove" {
		t.Fatalf("want Driver=trove, got %q", fe.Meta.Driver)
	}
}

func TestMapErr_StringNotFound(t *testing.T) {
	if !errors.Is(mapErr(errors.New(`mem: object "k" not found`)), fabriqerr.ErrNotFound) {
		t.Fatal("string object-not-found must classify as not_found")
	}
}

func TestMapErr_GenericIsInternalWithDetail(t *testing.T) {
	out := mapErr(errors.New("disk exploded"))
	var fe *fabriqerr.Error
	if !errors.As(out, &fe) || fe.Code != fabriqerr.CodeInternal {
		t.Fatalf("want CodeInternal, got %T %v", out, out)
	}
	if fe.Meta.Detail["driverMessage"] != "disk exploded" {
		t.Fatalf("raw message must be quarantined in Meta.Detail, got %+v", fe.Meta.Detail)
	}
	if fe.Error() == "disk exploded" || containsStr(fe.Error(), "disk exploded") {
		t.Fatalf("Error() must not contain raw backend text: %q", fe.Error())
	}
}

func TestMapErr_PassThroughStructured(t *testing.T) {
	if mapErr(nil) != nil {
		t.Fatal("nil must stay nil")
	}
	existing := fabriqerr.New(fabriqerr.CodeNotFound, "x")
	if got := mapErr(existing); !errors.Is(got, existing) {
		t.Fatal("already-structured error must pass through unchanged")
	}
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
