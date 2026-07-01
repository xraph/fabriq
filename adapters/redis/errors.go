package redis

import (
	"context"
	"errors"
	"net"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// translateRedis classifies a go-redis error into the fabriq taxonomy. It is
// nil-safe and idempotent: already-structured errors and fabriq sentinels pass
// through unchanged. The raw redis reply is quarantined in Meta.Detail.
func translateRedis(op string, err error) error {
	if err == nil {
		return nil
	}
	var fe *fabriqerr.Error
	if errors.As(err, &fe) {
		return err
	}
	if errors.Is(err, fabriqerr.ErrNotFound) || errors.Is(err, fabriqerr.ErrVersionConflict) {
		return err
	}

	code, retry := fabriqerr.CodeInternal, false
	switch {
	case errors.Is(err, goredis.Nil):
		code = fabriqerr.CodeNotFound
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		code = fabriqerr.CodeTimeout
	default:
		var ne net.Error
		msg := err.Error()
		switch {
		case errors.As(err, &ne),
			strings.Contains(msg, "connection refused"),
			strings.Contains(msg, "i/o timeout"),
			strings.Contains(msg, "no such host"),
			strings.Contains(msg, "EOF"):
			code, retry = fabriqerr.CodeUnavailable, true
		case strings.HasPrefix(msg, "NOAUTH"), strings.HasPrefix(msg, "WRONGPASS"):
			code = fabriqerr.CodePermissionDenied
		}
	}
	return fabriqerr.Wrap(code, err, fabriqerr.SafeMessage(code),
		fabriqerr.WithOp(op), fabriqerr.WithRetryable(retry),
		fabriqerr.WithMeta(fabriqerr.Meta{Driver: "redis",
			Detail: map[string]string{"driverMessage": err.Error()}}))
}
