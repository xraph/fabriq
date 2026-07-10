package remote

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// remoteSpatial is the client face of query.SpatialQuerier over the transport:
// Upsert/Delete ride a unary Ack, Within scans opaque-JSON rows like the
// relational reads, and Get has a dedicated reply (geometry + meta + a
// found/not-found bool, which RowReply can't carry).
type remoteSpatial struct{ t Transport }

var _ query.SpatialQuerier = remoteSpatial{}

func (r remoteSpatial) Upsert(ctx context.Context, entity, id string, geom query.Geometry, meta map[string]any) error {
	var metaJSON []byte
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		metaJSON = b
	}
	in, err := proto.Marshal(&fabriqpb.SpatialUpsertRequest{Entity: entity, Id: id, Wkt: geom.WKT, Srid: int32(geom.SRID), Meta: metaJSON}) //nolint:gosec // SRID is a small EPSG spatial-reference code, always within int32
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodSpatialUpsert, in)
	if err != nil {
		return err
	}
	return ackError(out)
}

func (r remoteSpatial) Within(ctx context.Context, q query.SpatialQuery, into any) error {
	body, err := json.Marshal(q)
	if err != nil {
		return err
	}
	in, err := proto.Marshal(&fabriqpb.SpatialWithinRequest{Query: body})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodSpatialWithin, in)
	if err != nil {
		return err
	}
	return scanRowReply(out, into)
}

//nolint:gocritic // multi-value result; the body returns explicit values and naming would collide with err/meta locals
func (r remoteSpatial) Get(ctx context.Context, entity, id string) (query.Geometry, map[string]any, bool, error) {
	in, err := proto.Marshal(&fabriqpb.SpatialGetRequest{Entity: entity, Id: id})
	if err != nil {
		return query.Geometry{}, nil, false, err
	}
	out, err := r.t.Unary(ctx, MethodSpatialGet, in)
	if err != nil {
		return query.Geometry{}, nil, false, err
	}
	var reply fabriqpb.SpatialGetReply
	if err := proto.Unmarshal(out, &reply); err != nil {
		return query.Geometry{}, nil, false, fmt.Errorf("remote: decode spatialGet reply: %w", err)
	}
	if reply.Error != nil {
		return query.Geometry{}, nil, false, errorFromProto(reply.Error)
	}
	var meta map[string]any
	if len(reply.Meta) > 0 {
		if err := json.Unmarshal(reply.Meta, &meta); err != nil {
			return query.Geometry{}, nil, false, fmt.Errorf("remote: decode spatial meta: %w", err)
		}
	}
	return query.Geometry{WKT: reply.Wkt, SRID: int(reply.Srid)}, meta, reply.Ok, nil
}

func (r remoteSpatial) Delete(ctx context.Context, entity, id string) error {
	in, err := proto.Marshal(&fabriqpb.SpatialDeleteRequest{Entity: entity, Id: id})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodSpatialDelete, in)
	if err != nil {
		return err
	}
	return ackError(out)
}
