package redis

import (
	"context"
	"errors"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/fabriq/core/fabriqerr"
)

func TestTranslateRedis(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code fabriqerr.Code
	}{
		{"nil", nil, ""},
		{"redis.Nil", goredis.Nil, fabriqerr.CodeNotFound},
		{"deadline", context.DeadlineExceeded, fabriqerr.CodeTimeout},
		{"conn refused", errors.New("dial tcp: connection refused"), fabriqerr.CodeUnavailable},
		{"noauth", errors.New("NOAUTH Authentication required"), fabriqerr.CodePermissionDenied},
		{"generic", errors.New("weird redis reply"), fabriqerr.CodeInternal},
	}
	for _, c := range cases {
		out := translateRedis("cache get", c.err)
		if c.err == nil {
			if out != nil {
				t.Errorf("%s: nil must stay nil", c.name)
			}
			continue
		}
		var fe *fabriqerr.Error
		if !errors.As(out, &fe) || fe.Code != c.code {
			t.Errorf("%s: got %T %v, want Code %q", c.name, out, out, c.code)
			continue
		}
		if fe.Meta.Driver != "redis" || fe.Op != "cache get" {
			t.Errorf("%s: meta/op wrong: %+v", c.name, fe)
		}
	}
}

func TestTranslateRedis_PassThrough(t *testing.T) {
	existing := fabriqerr.New(fabriqerr.CodeNotFound, "x")
	if got := translateRedis("x", existing); !errors.Is(got, existing) {
		t.Fatal("structured error must pass through")
	}
}
