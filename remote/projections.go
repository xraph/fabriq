package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	"google.golang.org/protobuf/proto"

	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// The projection-read ports — graph, search and vector — the agent toolkit's
// recall fuses (semantic / lexical / graph retrieval). Their results are
// canonical or map-native types (VectorMatch, search-hit maps, id/row slices),
// so they scan back as opaque JSON like the relational reads (scanRowReply).
// ApplyMutations is projection-plane-internal (consumers/rebuilds only) and
// Graph.TraverseAndHydrate infers the entity from a Go type the client cannot
// convey — recall does Query→ids→GetMany instead — so both stay unwired.

// --- vector (semantic channel) ---

type remoteVector struct{ t Transport }

var _ query.VectorQuerier = remoteVector{}

func (r remoteVector) Similar(ctx context.Context, q query.VectorQuery, into any) error {
	k := q.K
	if k < 0 {
		k = 0
	}
	if k > math.MaxInt32 {
		k = math.MaxInt32
	}
	in, err := proto.Marshal(&fabriqpb.VectorSimilarRequest{Entity: q.Entity, Embedding: q.Embedding, K: int32(k)})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodVectorSimilar, in)
	if err != nil {
		return err
	}
	return scanRowReply(out, into)
}

func (r remoteVector) Upsert(ctx context.Context, entity, id string, embedding []float32, meta map[string]any) error {
	var metaJSON []byte
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		metaJSON = b
	}
	in, err := proto.Marshal(&fabriqpb.VectorUpsertRequest{Entity: entity, Id: id, Embedding: embedding, Meta: metaJSON})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodVectorUpsert, in)
	if err != nil {
		return err
	}
	return ackError(out)
}

func (r remoteVector) Delete(ctx context.Context, entity, id string) error {
	in, err := proto.Marshal(&fabriqpb.VectorDeleteRequest{Entity: entity, Id: id})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodVectorDelete, in)
	if err != nil {
		return err
	}
	return ackError(out)
}

// --- search (lexical channel) ---

type remoteSearch struct{ t Transport }

var _ query.SearchQuerier = remoteSearch{}

func (r remoteSearch) Search(ctx context.Context, q query.SearchQuery, into any) error {
	body, err := json.Marshal(q)
	if err != nil {
		return err
	}
	in, err := proto.Marshal(&fabriqpb.SearchRequest{Query: body})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodSearch, in)
	if err != nil {
		return err
	}
	return scanRowReply(out, into)
}

// ApplyMutations is a projection-plane-internal write (consumers/rebuilds only).
func (r remoteSearch) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return ErrNotImplemented
}

// --- graph (graph channel) ---

type remoteGraph struct{ t Transport }

var _ query.GraphQuerier = remoteGraph{}

func (r remoteGraph) Query(ctx context.Context, cypher string, params map[string]any, into any) error {
	var paramsJSON []byte
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return err
		}
		paramsJSON = b
	}
	in, err := proto.Marshal(&fabriqpb.GraphQueryRequest{Cypher: cypher, Params: paramsJSON, Mode: graphMode(into)})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodGraphQuery, in)
	if err != nil {
		return err
	}
	return scanRowReply(out, into)
}

// TraverseAndHydrate infers the target entity from into's Go type, which the
// client cannot convey; recall does Query→ids→GetMany instead. Unwired.
func (r remoteGraph) TraverseAndHydrate(context.Context, string, map[string]any, any) error {
	return ErrNotImplemented
}

// ApplyMutations is projection-plane-internal (consumers/rebuilds only).
func (r remoteGraph) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return ErrNotImplemented
}

// graphMode picks the server scan shape from into: *[]string → "ids" (single-
// column id traversal), else "rows" (*[]map[string]any).
func graphMode(into any) string {
	t := reflect.TypeOf(into)
	if t != nil && t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Slice &&
		t.Elem().Elem().Kind() == reflect.String {
		return "ids"
	}
	return "rows"
}

// ackError decodes an Ack reply into its typed error (nil on success).
func ackError(out []byte) error {
	var reply fabriqpb.Ack
	if err := proto.Unmarshal(out, &reply); err != nil {
		return fmt.Errorf("remote: decode ack: %w", err)
	}
	return errorFromProto(reply.Error)
}
