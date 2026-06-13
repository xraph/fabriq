package fabriq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/adapters/redis"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// syncingDocStore decorates the Postgres document store with live
// fan-out: every appended update is published as a sync frame on the
// document's RAW channel (doc:{tenant}:{docID}), so collaborators see it
// immediately. Frames never touch the main event stream — projections
// only ever see materialization events.
type syncingDocStore struct {
	*postgres.DocStore
	pub *redis.Adapter
	reg *registry.Registry
	// authz is the document-plane hook (WithDocumentAuthz), set by Open
	// after the facade assembles its settings.
	authz func(ctx context.Context, docID string) error
}

var _ document.Store = (*syncingDocStore)(nil)

// ApplyUpdate appends, then fans the frame out. Fan-out is best-effort:
// the log is the truth and clients heal gaps via Sync (frames carry the
// log seq as Version for gap detection).
func (s *syncingDocStore) ApplyUpdate(ctx context.Context, docID string, update []byte) error {
	if s.authz != nil {
		if err := s.authz(ctx, docID); err != nil {
			return err
		}
	}
	seq, err := s.ApplyUpdateWithSeq(ctx, docID, update)
	if err != nil {
		return err
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	entity, _, _ := strings.Cut(docID, "/")
	_, _ = s.pub.PublishToChannel(ctx, registry.DocChannelName(tid, docID), event.Envelope{
		ID: event.NewID(), TenantID: tid, Aggregate: entity, AggID: docID,
		Version: seq, Type: registry.EventType(entity, "sync"),
		At: time.Now().UTC(), PayloadSchemaVersion: 1, Payload: json.RawMessage(update),
	})
	return nil
}

// SubscribeDocument attaches to a document's live sync frames: RAW
// delivery (every frame, in order, no conflation), channel resolved
// server-side from the validated doc id and the context tenant, through
// the same authz hook as Subscribe (scope name "doc"). Frame payloads are
// the update blobs; Version is the log seq — a gap means "call
// Document().Sync and resume".
func (f *Fabriq) SubscribeDocument(ctx context.Context, docID string) (<-chan query.Delta, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	entity, _, ok := strings.Cut(docID, "/")
	if !ok {
		return nil, fmt.Errorf("fabriq: document id %q must be <entity>/<id>", docID)
	}
	ent, found := f.reg.Get(entity)
	if !found || ent.Spec.Kind != registry.KindDocument {
		return nil, fmt.Errorf("fabriq: %q is not a registered document entity", entity)
	}
	if f.settings.docAuthz != nil {
		if authzErr := f.settings.docAuthz(ctx, docID); authzErr != nil {
			return nil, authzErr
		}
	}
	if f.settings.authz != nil {
		if authzErr := f.settings.authz(ctx, query.SubscribeScope{Entity: entity, Scope: "doc", ID: docID}); authzErr != nil {
			return nil, authzErr
		}
	}
	ch, _, err := f.hub.SubscribeRaw(ctx, registry.DocChannelName(tid, docID), f.settings.subscribeBuffer)
	return ch, err
}
