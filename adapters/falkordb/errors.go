package falkordb

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// translateGraph classifies a FalkorDB (RESP) error into the fabriq taxonomy.
// FalkorDB returns opaque string errors, so classification is coarse; the raw
// reply is quarantined in Meta.Detail. Nil-safe and idempotent.
func translateGraph(op string, err error) error {
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
	msg := strings.ToLower(err.Error())
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		code = fabriqerr.CodeTimeout
	case strings.Contains(msg, "syntax"),
		strings.Contains(msg, "invalid"),
		strings.Contains(msg, "not defined"),
		strings.Contains(msg, "type mismatch"):
		code = fabriqerr.CodeInvalidInput
	default:
		var ne net.Error
		if errors.As(err, &ne) ||
			strings.Contains(msg, "connection refused") ||
			strings.Contains(msg, "i/o timeout") ||
			strings.Contains(msg, "no such host") ||
			strings.Contains(msg, "eof") {
			code, retry = fabriqerr.CodeUnavailable, true
		}
	}
	return fabriqerr.Wrap(code, err, fabriqerr.SafeMessage(code),
		fabriqerr.WithOp(op), fabriqerr.WithRetryable(retry),
		fabriqerr.WithMeta(fabriqerr.Meta{Driver: "falkordb",
			Detail: map[string]string{"driverMessage": err.Error()}}))
}
