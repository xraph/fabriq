// core/agent/write.go
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// WriteError is the typed error a guarded write returns. Code is a stable,
// machine-readable reason; Err wraps the underlying cause (if any).
type WriteError struct {
	Code string
	Msg  string
	Err  error
}

func (e *WriteError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("agent: write %s: %s: %v", e.Code, e.Msg, e.Err)
	}
	return fmt.Sprintf("agent: write %s: %s", e.Code, e.Msg)
}
func (e *WriteError) Unwrap() error { return e.Err }

// RememberRequest is the input to a guarded write.
type RememberRequest struct {
	Entity          string          `json:"entity"`
	Op              string          `json:"op"` // create|update|upsert|delete
	AggID           string          `json:"aggId,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	ExpectedVersion *int64          `json:"expectedVersion,omitempty"`
}

func parseOp(s string) (command.Op, bool) {
	switch s {
	case "create":
		return command.OpCreate, true
	case "update":
		return command.OpUpdate, true
	case "upsert":
		return command.OpUpsert, true
	case "delete":
		return command.OpDelete, true
	}
	return 0, false
}

// decodePayload turns agent JSON into the entity's command payload: a typed
// model pointer for Go-model entities, or a map for dynamic entities.
func (t *Toolkit) decodePayload(entity string, raw json.RawMessage) (any, error) {
	ent, ok := t.reg.Get(entity)
	if !ok {
		return nil, fmt.Errorf("unknown entity %q", entity)
	}
	if ent.Binding.IsDynamic() {
		m := map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &m); err != nil {
				return nil, err
			}
		}
		return m, nil
	}
	ptr := reflect.New(ent.Binding.ModelType()).Interface()
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, ptr); err != nil {
			return nil, err
		}
	}
	return ptr, nil
}

// Remember performs a guarded write through the command plane. Power is
// WritePolicy ∩ tenant scope (inherited from ctx) ∩ lifecycle-hook rules
// (inherited from Exec). Errors are typed WriteErrors.
func (t *Toolkit) Remember(ctx context.Context, req RememberRequest) (command.Result, error) {
	op, ok := parseOp(req.Op)
	if !ok {
		return command.Result{}, &WriteError{Code: "validation_failed", Msg: fmt.Sprintf("unknown op %q", req.Op)}
	}
	if !t.cfg.Write.allows(req.Entity, op) {
		return command.Result{}, &WriteError{Code: "not_allowed", Msg: fmt.Sprintf("%s on %q not permitted", req.Op, req.Entity)}
	}
	var payload any
	if op != command.OpDelete {
		p, err := t.decodePayload(req.Entity, req.Payload)
		if err != nil {
			return command.Result{}, &WriteError{Code: "validation_failed", Msg: "payload", Err: err}
		}
		payload = p
	}
	res, err := t.fab.Exec(ctx, command.Command{
		Entity:          req.Entity,
		Op:              op,
		AggID:           req.AggID,
		Payload:         payload,
		ExpectedVersion: req.ExpectedVersion,
	})
	if err != nil {
		code := "exec_failed"
		if errors.Is(err, fabriqerr.ErrVersionConflict) {
			code = "version_conflict"
		} else {
			var nfe *fabriqerr.NotFoundError
			if errors.As(err, &nfe) {
				code = "not_found"
			}
		}
		return command.Result{}, &WriteError{Code: code, Msg: "exec", Err: err}
	}
	return res, nil
}
