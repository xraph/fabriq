package wardenauthz

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/warden"
)

func TestDefaultMapper(t *testing.T) {
	cases := []struct{ cap, action, resType string }{
		{"analytics.admin", "admin", "analytics"},
		{"connections.read", "read", "connections"},
		{"query", "query", ""},
	}
	for _, tc := range cases {
		a, rt, rid := DefaultMapper(context.Background(), tc.cap)
		if a != tc.action || rt != tc.resType || rid != "" {
			t.Errorf("DefaultMapper(%q) = (%q,%q,%q), want (%q,%q,\"\")", tc.cap, a, rt, rid, tc.action, tc.resType)
		}
	}
}

// fakeChecker forces a warden error to prove Authorize fails closed.
type fakeChecker struct {
	res *warden.CheckResult
	err error
}

func (f fakeChecker) Check(context.Context, *warden.CheckRequest, ...warden.CallOption) (*warden.CheckResult, error) {
	return f.res, f.err
}

func TestAuthorize_ErrorFailsClosed(t *testing.T) {
	a := &Authorizer{
		eng:     fakeChecker{res: &warden.CheckResult{Allowed: true}, err: errors.New("warden down")},
		mapCap:  DefaultMapper,
		subject: DefaultSubject,
	}
	ok, err := a.Authorize(context.Background(), "analytics.admin")
	if ok || err == nil {
		t.Fatalf("Authorize on engine error = (%v,%v), want (false, non-nil) — must not allow", ok, err)
	}
}
