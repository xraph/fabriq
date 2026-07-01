package adminapi

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// errorBody is the structured wire shape for a failed admin request.
type errorBody struct {
	Error errorPayload `json:"error"`
}

type errorPayload struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Entity    string         `json:"entity,omitempty"`
	ID        string         `json:"id,omitempty"`
	Op        string         `json:"op,omitempty"`
	Retryable bool           `json:"retryable"`
	Meta      fabriqerr.Meta `json:"meta,omitempty"`
}

// renderError writes a structured JSON error body plus the HTTP status implied
// by the error's Code. A raw driver string can never reach the client: any error
// that is not a *fabriqerr.Error (or a recognized legacy rich type) is rendered
// as a generic internal error.
func renderError(ctx forge.Context, err error) error {
	// Legacy rich types first — they predate the structured Error and are still
	// produced by the command plane and the Get path.
	var nf *fabriqerr.NotFoundError
	if errors.As(err, &nf) {
		return ctx.JSON(http.StatusNotFound, errorBody{Error: errorPayload{
			Code:    string(fabriqerr.CodeNotFound),
			Message: fabriqerr.SafeMessage(fabriqerr.CodeNotFound),
			Entity:  nf.Entity, ID: nf.ID,
		}})
	}
	var vc *fabriqerr.VersionConflictError
	if errors.As(err, &vc) {
		return ctx.JSON(http.StatusConflict, errorBody{Error: errorPayload{
			Code:    string(fabriqerr.CodeVersionConflict),
			Message: fabriqerr.SafeMessage(fabriqerr.CodeVersionConflict),
			Entity:  vc.Aggregate, ID: vc.AggID,
			Meta: fabriqerr.Meta{Detail: map[string]string{
				"expected": itoa(vc.Expected), "actual": itoa(vc.Actual),
			}},
		}})
	}

	var fe *fabriqerr.Error
	if errors.As(err, &fe) {
		return ctx.JSON(statusForCode(fe.Code), errorBody{Error: errorPayload{
			Code:      string(fe.Code),
			Message:   fe.Message,
			Entity:    fe.Entity,
			ID:        fe.ID,
			Op:        fe.Op,
			Retryable: fe.Retryable,
			Meta:      fe.Meta,
		}})
	}

	// Unstructured — never leak it. Generic 500.
	return ctx.JSON(http.StatusInternalServerError, errorBody{Error: errorPayload{
		Code:    string(fabriqerr.CodeInternal),
		Message: fabriqerr.SafeMessage(fabriqerr.CodeInternal),
	}})
}

func statusForCode(c fabriqerr.Code) int {
	switch c {
	case fabriqerr.CodeNotFound:
		return http.StatusNotFound
	case fabriqerr.CodeAlreadyExists, fabriqerr.CodeVersionConflict,
		fabriqerr.CodeConstraintViolation, fabriqerr.CodeSerialization:
		return http.StatusConflict
	case fabriqerr.CodeInvalidInput, fabriqerr.CodeSchemaMismatch:
		return http.StatusBadRequest
	case fabriqerr.CodeUnauthorized:
		return http.StatusUnauthorized
	case fabriqerr.CodePermissionDenied:
		return http.StatusForbidden
	case fabriqerr.CodeNotConfigured:
		return http.StatusNotImplemented
	case fabriqerr.CodeUnavailable:
		return http.StatusServiceUnavailable
	case fabriqerr.CodeTimeout:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

// unknownEntityCode reports whether err is the structured "unknown entity
// type" error produced by FakeRelational, the real adapter, and the command
// executor when an entity type name is not registered (fabriqerr.Error with
// Code == CodeInvalidInput and a non-empty Entity). It exists purely for the
// plugin routes' non-standard fallback behavior (return an empty list, or a
// 404, instead of the generic 400 renderError would otherwise produce) and
// should not be used as a general-purpose error mapper — renderError is.
func unknownEntityCode(err error) bool {
	var fe *fabriqerr.Error
	if !errors.As(err, &fe) {
		return false
	}
	return fe.Code == fabriqerr.CodeInvalidInput && fe.Entity != ""
}
