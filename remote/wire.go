package remote

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// The wire envelope is protobuf (remote/fabriqpb); the payload bodies inside it
// stay opaque JSON bytes — the registry-typed model on a Command, the ListQuery
// on a list, the row on a reply, the delta payload. The registry, not the wire
// schema, is the authority on payload shape (ADR 0009). These helpers convert
// between fabriq's domain types and the generated envelope; proto.Marshal /
// proto.Unmarshal of the envelope itself lives in client.go and server.go.

// Stable error codes carried in fabriqpb.Error.code.
const (
	codeNotFound        = "not_found"
	codeVersionConflict = "version_conflict"
	codeNoTenant        = "no_tenant"
	codeNotConfigured   = "not_configured"
	codeNotImplemented  = "not_implemented"
	codeInternal        = "internal"
)

func opToWire(o command.Op) (string, error) {
	switch o {
	case command.OpCreate:
		return "create", nil
	case command.OpUpdate:
		return "update", nil
	case command.OpDelete:
		return "delete", nil
	case command.OpUpsert:
		return "upsert", nil
	default:
		return "", fmt.Errorf("remote: unknown command op %d", int(o))
	}
}

func opFromWire(s string) (command.Op, error) {
	switch s {
	case "create":
		return command.OpCreate, nil
	case "update":
		return command.OpUpdate, nil
	case "delete":
		return command.OpDelete, nil
	case "upsert":
		return command.OpUpsert, nil
	default:
		return 0, fmt.Errorf("remote: unknown command op %q", s)
	}
}

// commandToProto builds the wire command; the payload (a registry-typed grove
// model) crosses as opaque JSON bytes.
func commandToProto(cmd command.Command) (*fabriqpb.Command, error) {
	op, err := opToWire(cmd.Op)
	if err != nil {
		return nil, err
	}
	pc := &fabriqpb.Command{
		Entity:          cmd.Entity,
		Op:              op,
		AggId:           cmd.AggID,
		ExpectedVersion: cmd.ExpectedVersion,
	}
	if cmd.Payload != nil {
		b, mErr := json.Marshal(cmd.Payload)
		if mErr != nil {
			return nil, fmt.Errorf("remote: marshal payload: %w", mErr)
		}
		pc.Payload = b
	}
	return pc, nil
}

func resultToProto(r command.Result) *fabriqpb.Result {
	return &fabriqpb.Result{AggId: r.AggID, Version: r.Version, EventId: r.EventID}
}

func resultFromProto(r *fabriqpb.Result) command.Result {
	if r == nil {
		return command.Result{}
	}
	return command.Result{AggID: r.AggId, Version: r.Version, EventID: r.EventId}
}

// errorToProto maps a fabriq error onto the wire taxonomy; nil stays nil.
func errorToProto(err error) *fabriqpb.Error {
	if err == nil {
		return nil
	}
	we := &fabriqpb.Error{Code: codeInternal, Message: err.Error()}
	switch {
	case errors.Is(err, fabriqerr.ErrVersionConflict):
		we.Code = codeVersionConflict
	case errors.Is(err, fabriqerr.ErrNotFound):
		we.Code = codeNotFound
	case errors.Is(err, tenant.ErrNoTenant):
		we.Code = codeNoTenant
	case errors.Is(err, fabriqerr.ErrStoreNotConfigured):
		we.Code = codeNotConfigured
	case errors.Is(err, ErrNotImplemented):
		we.Code = codeNotImplemented
	}
	return we
}

// errorFromProto rebuilds an errors.Is-matchable sentinel from the wire form.
// Rich variants (e.g. VersionConflictError's detail fields) are reconstructed as
// wrapped sentinels here; carrying their full fields is a follow-on.
func errorFromProto(we *fabriqpb.Error) error {
	if we == nil {
		return nil
	}
	switch we.Code {
	case codeVersionConflict:
		return fmt.Errorf("%s: %w", we.Message, fabriqerr.ErrVersionConflict)
	case codeNotFound:
		return fmt.Errorf("%s: %w", we.Message, fabriqerr.ErrNotFound)
	case codeNoTenant:
		return fmt.Errorf("%s: %w", we.Message, tenant.ErrNoTenant)
	case codeNotConfigured:
		return fmt.Errorf("%s: %w", we.Message, fabriqerr.ErrStoreNotConfigured)
	case codeNotImplemented:
		return fmt.Errorf("%s: %w", we.Message, ErrNotImplemented)
	default:
		return errors.New(we.Message)
	}
}

func scopeToProto(s query.SubscribeScope) *fabriqpb.SubscribeScope {
	return &fabriqpb.SubscribeScope{Entity: s.Entity, Scope: s.Scope, Id: s.ID}
}

func scopeFromProto(s *fabriqpb.SubscribeScope) query.SubscribeScope {
	if s == nil {
		return query.SubscribeScope{}
	}
	return query.SubscribeScope{Entity: s.Entity, Scope: s.Scope, ID: s.Id}
}

func deltaToProto(d query.Delta) *fabriqpb.Delta {
	pd := &fabriqpb.Delta{
		StreamId:  d.StreamID,
		Channel:   d.Channel,
		TenantId:  d.TenantID,
		Aggregate: d.Aggregate,
		AggId:     d.AggID,
		Version:   d.Version,
		Type:      d.Type,
		Payload:   d.Payload,
	}
	if !d.At.IsZero() {
		pd.AtUnixNano = d.At.UnixNano()
	}
	return pd
}

func deltaFromProto(d *fabriqpb.Delta) query.Delta {
	out := query.Delta{
		StreamID:  d.StreamId,
		Channel:   d.Channel,
		TenantID:  d.TenantId,
		Aggregate: d.Aggregate,
		AggID:     d.AggId,
		Version:   d.Version,
		Type:      d.Type,
		Payload:   d.Payload,
	}
	if d.AtUnixNano != 0 {
		out.At = time.Unix(0, d.AtUnixNano).UTC()
	}
	return out
}
