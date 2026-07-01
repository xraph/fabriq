package falkordb

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/fabriqerr"
)

func TestTranslateGraph(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code fabriqerr.Code
	}{
		{"nil", nil, ""},
		{"syntax", errors.New("errMsg: syntax error near 'MATCH'"), fabriqerr.CodeInvalidInput},
		{"deadline", context.DeadlineExceeded, fabriqerr.CodeTimeout},
		{"conn", errors.New("dial tcp: connection refused"), fabriqerr.CodeUnavailable},
		{"generic", errors.New("boom"), fabriqerr.CodeInternal},
	}
	for _, c := range cases {
		out := translateGraph("GRAPH.RO_QUERY g", c.err)
		if c.err == nil {
			if out != nil {
				t.Errorf("%s: nil must stay nil", c.name)
			}
			continue
		}
		var fe *fabriqerr.Error
		if !errors.As(out, &fe) || fe.Code != c.code {
			t.Errorf("%s: got %T %v, want %q", c.name, out, out, c.code)
			continue
		}
		if fe.Meta.Driver != "falkordb" || fe.Meta.Detail["driverMessage"] == "" {
			t.Errorf("%s: meta wrong: %+v", c.name, fe.Meta)
		}
	}
}
