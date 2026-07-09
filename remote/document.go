package remote

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// remoteDocStore is the client face of core/document.Store over the
// transport: ApplyUpdate/Compact ride a unary Ack, Sync has a dedicated
// reply (update bytes aren't a "row"), and Snapshot marshals the
// document.Materialized as opaque JSON into RowReply.Row — the client
// decodes it back into a fresh Materialized, not a caller-provided target
// (Snapshot's signature returns a value, unlike the row-scanning reads).
//
// Only the base Store is wired; the optional sub-interfaces (HistoryReader,
// SegmentLister, HistoryPurger) are a later increment — remoteDocStore does
// not assert them, so a type-assertion by a caller simply fails as "not
// supported", mirroring how the Blob plane handles optional caps.
type remoteDocStore struct{ t Transport }

var _ document.Store = remoteDocStore{}

func (r remoteDocStore) ApplyUpdate(ctx context.Context, docID string, update []byte) error {
	in, err := proto.Marshal(&fabriqpb.DocApplyUpdateRequest{DocId: docID, Update: update})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodDocApplyUpdate, in)
	if err != nil {
		return err
	}
	return ackError(out)
}

func (r remoteDocStore) Sync(ctx context.Context, docID string, stateVector []byte) ([]byte, error) {
	in, err := proto.Marshal(&fabriqpb.DocSyncRequest{DocId: docID, StateVector: stateVector})
	if err != nil {
		return nil, err
	}
	out, err := r.t.Unary(ctx, MethodDocSync, in)
	if err != nil {
		return nil, err
	}
	var reply fabriqpb.DocSyncReply
	if err := proto.Unmarshal(out, &reply); err != nil {
		return nil, fmt.Errorf("remote: decode docSync reply: %w", err)
	}
	if reply.Error != nil {
		return nil, errorFromProto(reply.Error)
	}
	return reply.Update, nil
}

func (r remoteDocStore) Snapshot(ctx context.Context, docID string) (document.Materialized, error) {
	in, err := proto.Marshal(&fabriqpb.DocSnapshotRequest{DocId: docID})
	if err != nil {
		return document.Materialized{}, err
	}
	out, err := r.t.Unary(ctx, MethodDocSnapshot, in)
	if err != nil {
		return document.Materialized{}, err
	}
	var reply fabriqpb.RowReply
	if err := proto.Unmarshal(out, &reply); err != nil {
		return document.Materialized{}, fmt.Errorf("remote: decode docSnapshot reply: %w", err)
	}
	if reply.Error != nil {
		return document.Materialized{}, errorFromProto(reply.Error)
	}
	var mat document.Materialized
	if err := json.Unmarshal(reply.Row, &mat); err != nil {
		return document.Materialized{}, fmt.Errorf("remote: decode materialized: %w", err)
	}
	return mat, nil
}

func (r remoteDocStore) Compact(ctx context.Context, docID string) error {
	in, err := proto.Marshal(&fabriqpb.DocCompactRequest{DocId: docID})
	if err != nil {
		return err
	}
	out, err := r.t.Unary(ctx, MethodDocCompact, in)
	if err != nil {
		return err
	}
	return ackError(out)
}
