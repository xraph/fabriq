package remote

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// remoteRelational is the client face of query.RelationalQuerier. Get, GetMany
// and List ride the unary transport: the request names the entity + id(s) or
// filter; the server runs the real read into a registry-typed scan target,
// marshals the row(s) to opaque JSON, and the client scans that JSON into the
// caller's `into` — the typed target the caller already holds, so no registry is
// needed client-side. Query (raw SQL) stays unwired by policy.
type remoteRelational struct{ t Transport }

var _ query.RelationalQuerier = remoteRelational{}

func (r remoteRelational) Get(ctx context.Context, entity, id string, into any) error {
	in, err := proto.Marshal(&fabriqpb.GetRequest{Entity: entity, Id: id})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodGet, in)
	if err != nil {
		return err
	}
	return scanRowReply(out, into)
}

func (r remoteRelational) GetMany(ctx context.Context, entity string, ids []string, into any) error {
	in, err := proto.Marshal(&fabriqpb.GetManyRequest{Entity: entity, Ids: ids})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodGetMany, in)
	if err != nil {
		return err
	}
	return scanRowReply(out, into)
}

// List sends the structured filter and scans the returned page into the
// caller's slice target. The query.ListQuery (a query.Cond struct tree) crosses
// as an opaque JSON body. NOTE: filter values cross as JSON, so numeric values
// arrive as float64 server-side — fine for string/bool filters; full numeric
// fidelity awaits modeling the filter in protobuf (ADR 0009).
func (r remoteRelational) List(ctx context.Context, entity string, q query.ListQuery, into any) error {
	body, err := json.Marshal(q)
	if err != nil {
		return err
	}
	in, err := proto.Marshal(&fabriqpb.ListRequest{Entity: entity, Query: body})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodList, in)
	if err != nil {
		return err
	}
	return scanRowReply(out, into)
}

// Query is the raw-SQL escape hatch. Remoting arbitrary SQL is a deliberate
// policy decision (it widens the surface well beyond the structured, validated
// filter), so it stays unwired pending that decision — use List for structured
// reads.
func (r remoteRelational) Query(context.Context, any, string, ...any) error {
	return ErrNotImplemented
}

// scanRowReply decodes the row envelope and unmarshals its opaque JSON body into
// the caller's target. An empty body (no row) is a no-op; a missing id surfaces
// as the server's typed ErrNotFound rather than a silent zero value.
func scanRowReply(out []byte, into any) error {
	var reply fabriqpb.RowReply
	if err := proto.Unmarshal(out, &reply); err != nil {
		return fmt.Errorf("remote: decode row reply: %w", err)
	}
	if reply.Error != nil {
		return errorFromProto(reply.Error)
	}
	if into == nil || len(reply.Row) == 0 {
		return nil
	}
	if err := json.Unmarshal(reply.Row, into); err != nil {
		return fmt.Errorf("remote: scan row into %T: %w", into, err)
	}
	return nil
}
