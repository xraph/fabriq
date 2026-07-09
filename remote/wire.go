package remote

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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

// listQueryToProto builds the structured wire filter from a query.ListQuery.
// Unlike the opaque-JSON `bytes query` field, every Cond.Value keeps its Go
// type across the wire via CondValue's oneof — fixing the int→float64
// fidelity loss JSON causes (JSON has no int).
func listQueryToProto(q query.ListQuery) *fabriqpb.ListQuery {
	pq := &fabriqpb.ListQuery{
		OrderBy: q.OrderBy,
		Limit:   int32(q.Limit),
		Offset:  int32(q.Offset),
	}
	if len(q.Where) > 0 {
		pq.Where = condsToProto(q.Where)
	}
	return pq
}

// listQueryFromProto is the inverse of listQueryToProto.
func listQueryFromProto(pq *fabriqpb.ListQuery) (query.ListQuery, error) {
	if pq == nil {
		return query.ListQuery{}, nil
	}
	where, err := condsFromProto(pq.Where)
	if err != nil {
		return query.ListQuery{}, err
	}
	return query.ListQuery{
		Where:   where,
		OrderBy: pq.OrderBy,
		Limit:   int(pq.Limit),
		Offset:  int(pq.Offset),
	}, nil
}

func condsToProto(conds []query.Cond) []*fabriqpb.Cond {
	out := make([]*fabriqpb.Cond, len(conds))
	for i, c := range conds {
		out[i] = condToProto(c)
	}
	return out
}

func condsFromProto(pcs []*fabriqpb.Cond) ([]query.Cond, error) {
	if len(pcs) == 0 {
		return nil, nil
	}
	out := make([]query.Cond, len(pcs))
	for i, pc := range pcs {
		c, err := condFromProto(pc)
		if err != nil {
			return nil, err
		}
		out[i] = c
	}
	return out, nil
}

// condToProto converts one query.Cond, recursing into Or groups.
func condToProto(c query.Cond) *fabriqpb.Cond {
	pc := &fabriqpb.Cond{
		Column: c.Column,
		Op:     string(c.Op),
	}
	if c.IsGroup() {
		pc.Or = condsToProto(c.Or)
		return pc
	}
	if c.Value != nil {
		pc.Value = condValueToProto(c.Value)
	}
	return pc
}

// condFromProto is the inverse of condToProto.
func condFromProto(pc *fabriqpb.Cond) (query.Cond, error) {
	if pc == nil {
		return query.Cond{}, nil
	}
	if len(pc.Or) > 0 {
		or, err := condsFromProto(pc.Or)
		if err != nil {
			return query.Cond{}, err
		}
		return query.Cond{Or: or}, nil
	}
	c := query.Cond{Column: pc.Column, Op: query.Op(pc.Op)}
	if pc.Value != nil {
		v, err := condValueFromProto(pc.Value)
		if err != nil {
			return query.Cond{}, err
		}
		c.Value = v
	}
	return c, nil
}

// condValueToProto converts a query.Cond.Value (an `any`: string/number/bool/
// []any for In/NotIn) to the wire's self-describing oneof scalar. Unknown
// types never panic or drop the value — they fall back to a string_val of
// fmt.Sprint(v).
func condValueToProto(v any) *fabriqpb.CondValue {
	switch tv := v.(type) {
	case int:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_IntVal{IntVal: int64(tv)}}
	case int8:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_IntVal{IntVal: int64(tv)}}
	case int16:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_IntVal{IntVal: int64(tv)}}
	case int32:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_IntVal{IntVal: int64(tv)}}
	case int64:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_IntVal{IntVal: tv}}
	case uint:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_IntVal{IntVal: int64(tv)}}
	case uint8:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_IntVal{IntVal: int64(tv)}}
	case uint16:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_IntVal{IntVal: int64(tv)}}
	case uint32:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_IntVal{IntVal: int64(tv)}}
	case uint64:
		// Cast to int64 for filter values; overflow guarding is unnecessary here.
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_IntVal{IntVal: int64(tv)}}
	case float32:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_DoubleVal{DoubleVal: float64(tv)}}
	case float64:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_DoubleVal{DoubleVal: tv}}
	case string:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_StringVal{StringVal: tv}}
	case bool:
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_BoolVal{BoolVal: tv}}
	case []any:
		items := make([]*fabriqpb.CondValue, len(tv))
		for i, item := range tv {
			items[i] = condValueToProto(item)
		}
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_ListVal{ListVal: &fabriqpb.ValueList{Items: items}}}
	default:
		// Go type switches do not match concrete slice types (e.g. []string)
		// against `case []any` — every real query.In/NotIn call site passes a
		// typed slice, not []any. Detect any slice/array kind via reflect (as
		// core/query/filter.go's In/NotIn validation already does) and recurse
		// element-by-element into list_val. A []byte is excluded — it is a
		// scalar blob, not a filter list.
		if rv := reflect.ValueOf(v); rv.IsValid() &&
			(rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) &&
			rv.Type() != reflect.TypeOf([]byte(nil)) {
			n := rv.Len()
			items := make([]*fabriqpb.CondValue, n)
			for i := 0; i < n; i++ {
				items[i] = condValueToProto(rv.Index(i).Interface())
			}
			return &fabriqpb.CondValue{V: &fabriqpb.CondValue_ListVal{ListVal: &fabriqpb.ValueList{Items: items}}}
		}
		// Unknown type (e.g. a caller-defined named type): never panic or drop
		// the value — fall back to its string form.
		return &fabriqpb.CondValue{V: &fabriqpb.CondValue_StringVal{StringVal: fmt.Sprint(v)}}
	}
}

// condValueFromProto is the inverse of condValueToProto: each oneof case maps
// to the Go type that preserves its wire fidelity (int64, float64, string,
// bool, []any) — never back to the original Go numeric width, which the wire
// does not carry.
func condValueFromProto(pv *fabriqpb.CondValue) (any, error) {
	if pv == nil {
		return nil, nil
	}
	switch v := pv.V.(type) {
	case *fabriqpb.CondValue_StringVal:
		return v.StringVal, nil
	case *fabriqpb.CondValue_DoubleVal:
		return v.DoubleVal, nil
	case *fabriqpb.CondValue_IntVal:
		return v.IntVal, nil
	case *fabriqpb.CondValue_BoolVal:
		return v.BoolVal, nil
	case *fabriqpb.CondValue_ListVal:
		items := v.ListVal.GetItems()
		out := make([]any, len(items))
		for i, item := range items {
			iv, err := condValueFromProto(item)
			if err != nil {
				return nil, err
			}
			out[i] = iv
		}
		return out, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("remote: unknown CondValue oneof case %T", v)
	}
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
