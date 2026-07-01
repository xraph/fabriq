package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// translateES classifies an Elasticsearch TRANSPORT error (from the client's
// Search/Bulk/Do call) — i.e. the request never produced an HTTP response.
// It is nil-safe and idempotent: an already-structured *fabriqerr.Error passes
// through unchanged. The raw client message is quarantined in Meta.Detail,
// never the caller-facing Message.
func translateES(op string, err error) error {
	if err == nil {
		return nil
	}
	var fe *fabriqerr.Error
	if errors.As(err, &fe) {
		return err
	}

	code, retry := fabriqerr.CodeInternal, false
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		code = fabriqerr.CodeTimeout
	default:
		var ne net.Error
		msg := err.Error()
		if errors.As(err, &ne) ||
			strings.Contains(msg, "connection refused") ||
			strings.Contains(msg, "i/o timeout") ||
			strings.Contains(msg, "no such host") ||
			strings.Contains(msg, "EOF") {
			code, retry = fabriqerr.CodeUnavailable, true
		}
	}
	return fabriqerr.Wrap(code, err, fabriqerr.SafeMessage(code),
		fabriqerr.WithOp(op), fabriqerr.WithRetryable(retry),
		fabriqerr.WithMeta(fabriqerr.Meta{Driver: "elastic",
			Detail: map[string]string{"driverMessage": err.Error()}}))
}

// translateESResponse classifies an Elasticsearch error RESPONSE (res.IsError()
// or a failed bulk item). It maps the HTTP status to a Code, refines by the
// parsed error.type, and keeps the raw body OUT of the caller-facing Message —
// only structured fields (status, type, reason) go into Meta.Detail.
func translateESResponse(op string, status int, body string) error {
	code := codeForESStatus(status)
	retry := status == 429 || status >= 500

	detail := map[string]string{"status": strconv.Itoa(status)}
	var parsed struct {
		Error struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(body), &parsed) == nil {
		if parsed.Error.Type != "" {
			detail["type"] = parsed.Error.Type
			if strings.Contains(parsed.Error.Type, "version_conflict") {
				code = fabriqerr.CodeVersionConflict
			}
		}
		if parsed.Error.Reason != "" {
			detail["reason"] = parsed.Error.Reason
		}
	}
	return fabriqerr.New(code, fabriqerr.SafeMessage(code),
		fabriqerr.WithOp(op), fabriqerr.WithRetryable(retry),
		fabriqerr.WithMeta(fabriqerr.Meta{Driver: "elastic", Detail: detail}))
}

// codeForESStatus maps an Elasticsearch HTTP status to a fabriq Code.
func codeForESStatus(status int) fabriqerr.Code {
	switch status {
	case 400:
		return fabriqerr.CodeInvalidInput
	case 401:
		return fabriqerr.CodeUnauthorized
	case 403:
		return fabriqerr.CodePermissionDenied
	case 404:
		return fabriqerr.CodeNotFound
	case 409:
		return fabriqerr.CodeAlreadyExists
	case 429:
		return fabriqerr.CodeUnavailable
	}
	if status >= 500 {
		return fabriqerr.CodeUnavailable
	}
	return fabriqerr.CodeInternal
}
