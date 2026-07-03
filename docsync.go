package fabriq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
// seqDocStore is the document store shape the fan-out decorator needs:
// the port plus the seq-returning apply (gap detection on live frames).
type seqDocStore interface {
	document.Store
	ApplyUpdateWithSeq(ctx context.Context, docID string, update []byte) (int64, error)
}

type syncingDocStore struct {
	seqDocStore
	pub *redis.Adapter
	reg *registry.Registry
	// authz is the document-plane hook (WithDocumentAuthz), set by Open
	// after the facade assembles its settings.
	authz func(ctx context.Context, docID string) error
	// wake nudges the catalog-mode sweeper after an append (nil in static
	// mode, where the boot-time materializer polls on its own).
	wake func(ctx context.Context, tenantID string)
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
	if s.wake != nil {
		s.wake(ctx, tid)
	}
	return nil
}

// SubscribeDocument attaches to a document's live sync frames: RAW
// delivery (every frame, in order, no conflation), channel resolved
// server-side from the validated doc id and the context tenant, through
// the same authz hook as Subscribe (scope name "doc"). Frame payloads are
// the update blobs; Version is the log seq — a gap means "call
// Document().Sync and resume".
func (f *Fabriq) SubscribeDocument(ctx context.Context, docID string) (<-chan query.Delta, error) {
	tid, _, err := f.validateDocAccess(ctx, docID)
	if err != nil {
		return nil, err
	}
	ch, _, err := f.hub.SubscribeRaw(ctx, registry.DocChannelName(tid, docID), f.settings.subscribeBuffer)
	return ch, err
}

// validateDocAccess runs the document-plane authz gauntlet shared by the
// awareness methods: id shape, registered KindDocument entity, docAuthz,
// then the generic subscribe authz under the "doc" scope.
func (f *Fabriq) validateDocAccess(ctx context.Context, docID string) (tenantID, entity string, err error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return "", "", err
	}
	entity, _, ok := strings.Cut(docID, "/")
	if !ok {
		return "", "", fmt.Errorf("fabriq: document id %q must be <entity>/<id>", docID)
	}
	ent, found := f.reg.Get(entity)
	if !found || ent.Spec.Kind != registry.KindDocument {
		return "", "", fmt.Errorf("fabriq: %q is not a registered document entity", entity)
	}
	if f.settings.docAuthz != nil {
		if authzErr := f.settings.docAuthz(ctx, docID); authzErr != nil {
			return "", "", authzErr
		}
	}
	if f.settings.authz != nil {
		if authzErr := f.settings.authz(ctx, query.SubscribeScope{Entity: entity, Scope: "doc", ID: docID}); authzErr != nil {
			return "", "", authzErr
		}
	}
	return tid, entity, nil
}

// presencePublisher is the optional capability the live (Redis-backed)
// document store implements; stores without a transport don't.
type presencePublisher interface {
	publishPresence(ctx context.Context, tenantID, entity, docID, nodeID string, data json.RawMessage) error
}

// presenceFrame is the awareness wire payload: the publishing peer plus
// its opaque state (cursor, selection, profile — whatever the app sends).
type presenceFrame struct {
	Node string          `json:"node"`
	Data json.RawMessage `json:"data,omitempty"`
}

// publishPresence implements presencePublisher on the syncing store: one
// ephemeral frame on the document's awareness channel.
func (s *syncingDocStore) publishPresence(ctx context.Context, tenantID, entity, docID, nodeID string, data json.RawMessage) error {
	payload, err := json.Marshal(presenceFrame{Node: nodeID, Data: data})
	if err != nil {
		return err
	}
	_, err = s.pub.PublishToChannel(ctx, registry.DocPresenceChannelName(tenantID, docID), event.Envelope{
		ID: event.NewID(), TenantID: tenantID, Aggregate: entity, AggID: docID,
		Type: registry.EventType(entity, "presence"),
		At:   time.Now().UTC(), PayloadSchemaVersion: 1, Payload: payload,
	})
	return err
}

// PublishDocumentPresence broadcasts one ephemeral awareness frame
// (cursor, selection, who's-online — any opaque payload) to a document's
// collaborators. Presence rides a capped Redis stream tailed from "now":
// never persisted, no delivery guarantees, gone when it ages out — by
// design (see core/document DESIGN.md).
func (f *Fabriq) PublishDocumentPresence(ctx context.Context, docID, nodeID string, data json.RawMessage) error {
	tid, entity, err := f.validateDocAccess(ctx, docID)
	if err != nil {
		return err
	}
	if nodeID == "" {
		return fmt.Errorf("fabriq: presence requires a node id")
	}
	pub, ok := f.ports.Documents.(presencePublisher)
	if !ok {
		return fmt.Errorf("fabriq: document presence requires the live document plane (Redis transport)")
	}
	return pub.publishPresence(ctx, tid, entity, docID, nodeID, data)
}

// SubscribeDocumentPresence attaches to a document's live awareness
// frames: RAW delivery, resolved server-side from the validated doc id
// and the context tenant, through the same authz as SubscribeDocument.
// Frame payloads are the opaque presence blobs peers published.
func (f *Fabriq) SubscribeDocumentPresence(ctx context.Context, docID string) (<-chan query.Delta, error) {
	tid, _, err := f.validateDocAccess(ctx, docID)
	if err != nil {
		return nil, err
	}
	ch, _, err := f.hub.SubscribeRaw(ctx, registry.DocPresenceChannelName(tid, docID), f.settings.subscribeBuffer)
	return ch, err
}
