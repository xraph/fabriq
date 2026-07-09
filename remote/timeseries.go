package remote

import (
	"context"
	"encoding/json"

	"google.golang.org/protobuf/proto"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// remoteTS is the client face of query.TSQuerier over the transport: BulkWrite
// rides a unary Ack, Range scans opaque-JSON rows like the relational reads.
type remoteTS struct{ t Transport }

var _ query.TSQuerier = remoteTS{}

func (r remoteTS) BulkWrite(ctx context.Context, series string, points []query.Point) error {
	body, err := json.Marshal(points)
	if err != nil {
		return err
	}
	in, err := proto.Marshal(&fabriqpb.TSBulkWriteRequest{Series: series, Points: body})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodTSBulkWrite, in)
	if err != nil {
		return err
	}
	return ackError(out)
}

func (r remoteTS) Range(ctx context.Context, q query.RangeQuery, into any) error {
	body, err := json.Marshal(q)
	if err != nil {
		return err
	}
	in, err := proto.Marshal(&fabriqpb.TSRangeRequest{Query: body})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodTSRange, in)
	if err != nil {
		return err
	}
	return scanRowReply(out, into)
}
