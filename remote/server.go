package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// Handler is the server face: it terminates the remote envelope and delegates
// to a real, embedded query.Fabric (an Open()ed *fabriq.Fabriq on the
// connection-owning tier). The gRPC service implementation calls Dispatch /
// DispatchStream (or the per-method handlers) from its generated stubs.
//
// SECURITY: ctx MUST already carry the tenant and principal resolved from the
// call's transport metadata by an auth interceptor — never from a field in the
// decoded message. The embedded facade then enforces RLS and authz exactly as
// it does in-process (ADR 0009 §Security).
type Handler struct {
	fab  query.Fabric
	reg  *registry.Registry
	live LiveQuerier // nil unless the facade implements LiveQuery
}

// NewHandler builds a Handler over the embedded facade and its registry (the
// registry is the schema authority used to decode opaque payloads). If the
// facade also implements LiveQuerier (the concrete *fabriq.Fabriq does), the
// remote LiveQuery plane is enabled; otherwise it returns ErrNotImplemented.
func NewHandler(fab query.Fabric, reg *registry.Registry) *Handler {
	h := &Handler{fab: fab, reg: reg}
	// The facade may satisfy LiveQuerier directly (a test double returning
	// LiveSubscription) or the concrete shape (*fabriq.Fabriq, returning
	// *livequery.Handle) which the adapter widens.
	if lq, ok := fab.(LiveQuerier); ok {
		h.live = lq
	} else if clq, ok := fab.(concreteLiveQuerier); ok {
		h.live = liveQuerierAdapter{c: clq}
	}
	return h
}

// Dispatch routes a unary call by its method name — the server-side mirror of
// Transport.Unary, used by Loopback and by a thin gRPC unary shim.
func (h *Handler) Dispatch(ctx context.Context, method string, in []byte) ([]byte, error) {
	switch method {
	case MethodExec:
		return h.Exec(ctx, in)
	case MethodExecBatch:
		return h.ExecBatch(ctx, in)
	case MethodGet:
		return h.Get(ctx, in)
	case MethodGetMany:
		return h.GetMany(ctx, in)
	case MethodList:
		return h.List(ctx, in)
	case MethodHeadBlob:
		return h.HeadBlob(ctx, in)
	case MethodDeleteBlob:
		return h.DeleteBlob(ctx, in)
	case MethodPresignBlob:
		return h.PresignBlob(ctx, in)
	case MethodListBlob:
		return h.ListBlob(ctx, in)
	case MethodCopyBlob:
		return h.CopyBlob(ctx, in)
	case MethodVectorSimilar:
		return h.VectorSimilar(ctx, in)
	case MethodVectorUpsert:
		return h.VectorUpsert(ctx, in)
	case MethodVectorDelete:
		return h.VectorDelete(ctx, in)
	case MethodVectorDeleteByMeta:
		return h.VectorDeleteByMeta(ctx, in)
	case MethodVectorGet:
		return h.VectorGet(ctx, in)
	case MethodSearch:
		return h.Search(ctx, in)
	case MethodGraphQuery:
		return h.GraphQuery(ctx, in)
	case MethodTSBulkWrite:
		return h.TSBulkWrite(ctx, in)
	case MethodTSRange:
		return h.TSRange(ctx, in)
	case MethodSpatialUpsert:
		return h.SpatialUpsert(ctx, in)
	case MethodSpatialWithin:
		return h.SpatialWithin(ctx, in)
	case MethodSpatialGet:
		return h.SpatialGet(ctx, in)
	case MethodSpatialDelete:
		return h.SpatialDelete(ctx, in)
	case MethodDocApplyUpdate:
		return h.DocApplyUpdate(ctx, in)
	case MethodDocSync:
		return h.DocSync(ctx, in)
	case MethodDocSnapshot:
		return h.DocSnapshot(ctx, in)
	case MethodDocCompact:
		return h.DocCompact(ctx, in)
	default:
		return nil, fmt.Errorf("remote: unknown method %q", method)
	}
}

// DispatchStream routes a server-streaming call by method name. send delivers
// one frame; the error it returns (e.g. client gone) aborts the stream.
func (h *Handler) DispatchStream(ctx context.Context, method string, in []byte, send func([]byte) error) error {
	switch method {
	case MethodSubscribe:
		return h.Subscribe(ctx, in, send)
	case MethodGetBlob:
		return h.GetBlob(ctx, in, send)
	default:
		return fmt.Errorf("remote: unknown stream method %q", method)
	}
}

// DispatchBidi routes a bidirectional call by method name. recv returns the next
// client frame (io.EOF when the client is done sending); send delivers one frame
// to the client. The handler returns when the call is over; the transport binding
// turns a returned error into the stream status.
func (h *Handler) DispatchBidi(ctx context.Context, method string, recv func() ([]byte, error), send func([]byte) error) error {
	switch method {
	case MethodBidiEcho:
		return h.BidiEcho(ctx, recv, send)
	case MethodLiveQuery:
		return h.LiveQuery(ctx, recv, send)
	default:
		return fmt.Errorf("remote: unknown bidi method %q", method)
	}
}

// BidiEcho is a diagnostic bidirectional handler: it echoes each received frame
// back to the client prefixed with "echo:", until the client stops sending
// (io.EOF) or the call is cancelled. It exists to exercise the bidi transport
// primitive on its own — the live-query plane is the real consumer.
func (h *Handler) BidiEcho(ctx context.Context, recv func() ([]byte, error), send func([]byte) error) error {
	for {
		frame, err := recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := send(append([]byte("echo:"), frame...)); err != nil {
			return err
		}
	}
}

// Exec decodes one command, rebuilds its registry-typed payload, runs it on the
// embedded facade, and encodes the result or typed error.
func (h *Handler) Exec(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.ExecRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode exec request: %w", err)
	}
	cmd, err := h.commandFromProto(req.Command)
	if err != nil {
		return proto.Marshal(&fabriqpb.ExecReply{Error: errorToProto(err)})
	}
	res, err := h.fab.Exec(ctx, cmd)
	if err != nil {
		return proto.Marshal(&fabriqpb.ExecReply{Error: errorToProto(err)})
	}
	return proto.Marshal(&fabriqpb.ExecReply{Result: resultToProto(res)})
}

// ExecBatch decodes N commands and runs them in one server-side transaction.
func (h *Handler) ExecBatch(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.ExecBatchRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode execBatch request: %w", err)
	}
	cmds := make([]command.Command, len(req.Commands))
	for i, pc := range req.Commands {
		cmd, err := h.commandFromProto(pc)
		if err != nil {
			return proto.Marshal(&fabriqpb.ExecBatchReply{Error: errorToProto(err)})
		}
		cmds[i] = cmd
	}
	results, err := h.fab.ExecBatch(ctx, cmds)
	if err != nil {
		return proto.Marshal(&fabriqpb.ExecBatchReply{Error: errorToProto(err)})
	}
	prs := make([]*fabriqpb.Result, len(results))
	for i, r := range results {
		prs[i] = resultToProto(r)
	}
	return proto.Marshal(&fabriqpb.ExecBatchReply{Results: prs})
}

// Get is the server side of MethodGet: build a registry-typed scan target, run
// the real relational read, and return the row as opaque JSON.
func (h *Handler) Get(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.GetRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode get request: %w", err)
	}
	ent, ok := h.reg.Get(req.Entity)
	if !ok {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(fmt.Errorf("remote: unknown entity %q", req.Entity))})
	}
	target := newOne(ent)
	if err := h.fab.Relational().Get(ctx, req.Entity, req.Id, target); err != nil {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(err)})
	}
	row, err := json.Marshal(target)
	if err != nil {
		return nil, fmt.Errorf("remote: marshal row: %w", err)
	}
	return proto.Marshal(&fabriqpb.RowReply{Row: row})
}

// GetMany is the server side of MethodGetMany: the batched (no-N+1) read.
func (h *Handler) GetMany(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.GetManyRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode getMany request: %w", err)
	}
	ent, ok := h.reg.Get(req.Entity)
	if !ok {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(fmt.Errorf("remote: unknown entity %q", req.Entity))})
	}
	target := newMany(ent)
	if err := h.fab.Relational().GetMany(ctx, req.Entity, req.Ids, target); err != nil {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(err)})
	}
	rows, err := json.Marshal(target)
	if err != nil {
		return nil, fmt.Errorf("remote: marshal rows: %w", err)
	}
	return proto.Marshal(&fabriqpb.RowReply{Row: rows})
}

// List is the server side of MethodList: decode the structured filter —
// preferring the typed `structured` field (full numeric fidelity) and
// falling back to the legacy opaque-JSON `query` body when structured is
// unset (back-compat with pre-Task-C clients for one release) — run the real
// paged read into a registry-typed slice target, and return opaque-JSON rows.
func (h *Handler) List(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.ListRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode list request: %w", err)
	}
	ent, ok := h.reg.Get(req.Entity)
	if !ok {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(fmt.Errorf("remote: unknown entity %q", req.Entity))})
	}
	var q query.ListQuery
	switch {
	case req.Structured != nil:
		var err error
		q, err = listQueryFromProto(req.Structured)
		if err != nil {
			return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(fmt.Errorf("remote: decode structured list query: %w", err))})
		}
	case len(req.Query) > 0:
		if err := json.Unmarshal(req.Query, &q); err != nil {
			return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(fmt.Errorf("remote: decode list query: %w", err))})
		}
	}
	target := newMany(ent)
	if err := h.fab.Relational().List(ctx, req.Entity, q, target); err != nil {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(err)})
	}
	rows, err := json.Marshal(target)
	if err != nil {
		return nil, fmt.Errorf("remote: marshal rows: %w", err)
	}
	return proto.Marshal(&fabriqpb.RowReply{Row: rows})
}

// Subscribe is the server side of MethodSubscribe. The embedded facade resolves
// the scope (authz + channel resolution happen there); the first frame is a
// handshake reporting setup success or a typed error so the client can honor
// Subscribe's synchronous-error contract, then one Delta per frame follows.
func (h *Handler) Subscribe(ctx context.Context, in []byte, send func([]byte) error) error {
	var req fabriqpb.SubscribeRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return fmt.Errorf("remote: decode subscribe request: %w", err)
	}
	ch, err := h.fab.Subscribe(ctx, scopeFromProto(req.Scope))
	if err != nil {
		return sendProto(send, &fabriqpb.SubFrame{Error: errorToProto(err)})
	}
	if err := sendProto(send, &fabriqpb.SubFrame{Open: true}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-ch:
			if !ok {
				return nil
			}
			if err := sendProto(send, &fabriqpb.SubFrame{Delta: deltaToProto(d)}); err != nil {
				return err
			}
		}
	}
}

// LiveQuery is the server side of MethodLiveQuery, a bidirectional stream. The
// client's first frame carries the query; it opens a maintained-window
// subscription on the facade, sends the snapshot as the first server frame, then
// drains deltas to the client in a goroutine. Meanwhile it loops on recv for
// control frames: a Reanchor frame slides the window (in-process Handle.Reanchor)
// and returns the fresh snapshot as a server frame. It exits — Closing the engine
// handle — when the client stops sending (io.EOF), disconnects (ctx.Done), or the
// delta channel closes. A facade without LiveQuery answers ErrNotImplemented.
//
// SendMsg is not safe for concurrent use, so a mutex serializes the delta pump
// and the reanchor replies onto the single send func.
func (h *Handler) LiveQuery(ctx context.Context, recv func() ([]byte, error), send func([]byte) error) error {
	var sendMu sync.Mutex
	safeSend := func(m proto.Message) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return sendProto(send, m)
	}

	first, err := recv()
	if err != nil {
		return err
	}
	var cf fabriqpb.LiveClientFrame
	if err := proto.Unmarshal(first, &cf); err != nil {
		return fmt.Errorf("remote: decode live client frame: %w", err)
	}
	if h.live == nil {
		return safeSend(&fabriqpb.LiveFrame{Error: errorToProto(fmt.Errorf("remote: live queries not configured: %w", ErrNotImplemented))})
	}
	var q livequery.LiveQuery
	if cf.Query != nil && len(cf.Query.Query) > 0 {
		if err := json.Unmarshal(cf.Query.Query, &q); err != nil {
			return safeSend(&fabriqpb.LiveFrame{Error: errorToProto(fmt.Errorf("remote: decode live query: %w", err))})
		}
	}
	snap, deltas, sub, err := h.live.LiveQuery(ctx, q)
	if err != nil {
		return safeSend(&fabriqpb.LiveFrame{Error: errorToProto(err)})
	}
	if sub != nil {
		defer sub.Close()
	}
	snapBody, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := safeSend(&fabriqpb.LiveFrame{Snapshot: snapBody}); err != nil {
		return err
	}

	// ctx bounds both the delta pump and the recv reader; either exiting cancels
	// the other so neither goroutine leaks.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Delta pump: one server frame per delta until the channel closes (clean end)
	// or a send fails.
	pumpDone := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				pumpDone <- nil
				return
			case d, ok := <-deltas:
				if !ok {
					pumpDone <- nil // the subscription ended: clean stream close
					return
				}
				body, merr := json.Marshal(d)
				if merr != nil {
					pumpDone <- merr
					return
				}
				if serr := safeSend(&fabriqpb.LiveFrame{Delta: body}); serr != nil {
					pumpDone <- serr
					return
				}
			}
		}
	}()

	// Recv reader: surface each client frame (or the terminal recv error) on a
	// channel so the control loop can select it against pump completion.
	type recvResult struct {
		frame []byte
		err   error
	}
	frames := make(chan recvResult)
	go func() {
		for {
			frame, rerr := recv()
			select {
			case frames <- recvResult{frame: frame, err: rerr}:
			case <-ctx.Done():
				return
			}
			if rerr != nil {
				return
			}
		}
	}()

	// Control loop: a Reanchor client frame slides the window and replies with the
	// fresh snapshot; io.EOF (client Close) or pump completion ends the stream.
	for {
		select {
		case perr := <-pumpDone:
			return perr
		case rr := <-frames:
			if rr.err != nil {
				if errors.Is(rr.err, io.EOF) {
					return nil
				}
				return rr.err
			}
			var ctrl fabriqpb.LiveClientFrame
			if uerr := proto.Unmarshal(rr.frame, &ctrl); uerr != nil {
				return fmt.Errorf("remote: decode live control frame: %w", uerr)
			}
			if ctrl.Reanchor == nil {
				continue // ignore unknown/empty control frames
			}
			var cursor *livequery.Cursor
			if len(ctrl.Reanchor.Cursor) > 0 {
				cursor = &livequery.Cursor{}
				if uerr := json.Unmarshal(ctrl.Reanchor.Cursor, cursor); uerr != nil {
					if serr := safeSend(&fabriqpb.LiveFrame{Error: errorToProto(fmt.Errorf("remote: decode reanchor cursor: %w", uerr))}); serr != nil {
						return serr
					}
					continue
				}
			}
			if sub == nil {
				if serr := safeSend(&fabriqpb.LiveFrame{Error: errorToProto(ErrNotImplemented)}); serr != nil {
					return serr
				}
				continue
			}
			rsnap, rerr2 := sub.Reanchor(ctx, cursor, int(ctrl.Reanchor.Limit))
			if rerr2 != nil {
				if serr := safeSend(&fabriqpb.LiveFrame{Error: errorToProto(rerr2)}); serr != nil {
					return serr
				}
				continue
			}
			rbody, merr := json.Marshal(rsnap)
			if merr != nil {
				return merr
			}
			if serr := safeSend(&fabriqpb.LiveFrame{Snapshot: rbody}); serr != nil {
				return serr
			}
		}
	}
}

// --- knowledge retrieval: the projection-read channels recall fuses. Each runs
// the real port read into a canonical/map-native target and returns it as
// opaque JSON. ---

func (h *Handler) VectorSimilar(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.VectorSimilarRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode vectorSimilar request: %w", err)
	}
	vq := query.VectorQuery{Entity: req.Entity, Embedding: req.Embedding, K: int(req.K)}
	if len(req.Filter) > 0 {
		if err := json.Unmarshal(req.Filter, &vq.Filter); err != nil {
			return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(fmt.Errorf("remote: decode vector filter: %w", err))})
		}
	}
	var matches []query.VectorMatch
	if err := h.fab.Vector().Similar(ctx, vq, &matches); err != nil {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(err)})
	}
	row, err := json.Marshal(matches)
	if err != nil {
		return nil, fmt.Errorf("remote: marshal matches: %w", err)
	}
	return proto.Marshal(&fabriqpb.RowReply{Row: row})
}

func (h *Handler) VectorDeleteByMeta(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.VectorDeleteByMetaRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode vectorDeleteByMeta request: %w", err)
	}
	var filter map[string]string
	if len(req.Filter) > 0 {
		if err := json.Unmarshal(req.Filter, &filter); err != nil {
			return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(fmt.Errorf("remote: decode deleteByMeta filter: %w", err))})
		}
	}
	return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(h.fab.Vector().DeleteByMeta(ctx, req.Entity, filter))})
}

func (h *Handler) VectorUpsert(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.VectorUpsertRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode vectorUpsert request: %w", err)
	}
	var meta map[string]any
	if len(req.Meta) > 0 {
		if err := json.Unmarshal(req.Meta, &meta); err != nil {
			return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(fmt.Errorf("remote: decode meta: %w", err))})
		}
	}
	return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(h.fab.Vector().Upsert(ctx, req.Entity, req.Id, req.Embedding, meta))})
}

func (h *Handler) VectorDelete(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.VectorDeleteRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode vectorDelete request: %w", err)
	}
	return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(h.fab.Vector().Delete(ctx, req.Entity, req.Id))})
}

func (h *Handler) VectorGet(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.VectorGetRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode vectorGet request: %w", err)
	}
	emb, err := h.fab.Vector().Get(ctx, req.Entity, req.Id)
	if err != nil {
		return proto.Marshal(&fabriqpb.VectorGetReply{Error: errorToProto(err)})
	}
	return proto.Marshal(&fabriqpb.VectorGetReply{Embedding: emb})
}

func (h *Handler) Search(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.SearchRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode search request: %w", err)
	}
	var q query.SearchQuery
	if len(req.Query) > 0 {
		if err := json.Unmarshal(req.Query, &q); err != nil {
			return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(fmt.Errorf("remote: decode search query: %w", err))})
		}
	}
	var hits []map[string]any
	if err := h.fab.Search().Search(ctx, q, &hits); err != nil {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(err)})
	}
	row, err := json.Marshal(hits)
	if err != nil {
		return nil, fmt.Errorf("remote: marshal hits: %w", err)
	}
	return proto.Marshal(&fabriqpb.RowReply{Row: row})
}

func (h *Handler) GraphQuery(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.GraphQueryRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode graphQuery request: %w", err)
	}
	var params map[string]any
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(fmt.Errorf("remote: decode params: %w", err))})
		}
	}
	// The client's `into` shape (ids vs rows) is conveyed via mode; build the
	// matching scan target so the graph adapter dispatches correctly.
	var into any
	if req.Mode == "ids" {
		into = &[]string{}
	} else {
		into = &[]map[string]any{}
	}
	if err := h.fab.Graph().Query(ctx, req.Cypher, params, into); err != nil {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(err)})
	}
	row, err := json.Marshal(into)
	if err != nil {
		return nil, fmt.Errorf("remote: marshal graph rows: %w", err)
	}
	return proto.Marshal(&fabriqpb.RowReply{Row: row})
}

// DispatchClientStream routes a client-streaming call by method name. recv
// returns the next request frame, or io.EOF when the client is done; the handler
// returns the single response frame.
func (h *Handler) DispatchClientStream(ctx context.Context, method string, recv func() ([]byte, error)) ([]byte, error) {
	switch method {
	case MethodPutBlob:
		return h.PutBlob(ctx, recv)
	default:
		return nil, fmt.Errorf("remote: unknown client-stream method %q", method)
	}
}

// PutBlob is the server side of MethodPutBlob: the first frame carries metadata,
// the rest carry bytes, which it pipes into the byte store's streaming Put.
func (h *Handler) PutBlob(ctx context.Context, recv func() ([]byte, error)) ([]byte, error) {
	first, err := recv()
	if err != nil {
		return nil, fmt.Errorf("remote: put: missing metadata frame: %w", err)
	}
	var meta fabriqpb.BlobChunk
	if err = proto.Unmarshal(first, &meta); err != nil {
		return nil, fmt.Errorf("remote: decode put metadata: %w", err)
	}
	pr, pw := io.Pipe()
	go func() {
		for {
			frame, rerr := recv()
			if errors.Is(rerr, io.EOF) {
				_ = pw.Close()
				return
			}
			if rerr != nil {
				_ = pw.CloseWithError(rerr)
				return
			}
			var chunk fabriqpb.BlobChunk
			if uerr := proto.Unmarshal(frame, &chunk); uerr != nil {
				_ = pw.CloseWithError(uerr)
				return
			}
			if len(chunk.Data) > 0 {
				if _, werr := pw.Write(chunk.Data); werr != nil {
					return // reader gone
				}
			}
		}
	}()
	info, err := h.fab.Blob().Put(ctx, meta.Key, pr, blob.PutOpts{ContentType: meta.ContentType, Size: meta.Size})
	_ = pr.Close() // unblock the feeder if Put returned early
	if err != nil {
		return proto.Marshal(&fabriqpb.BlobInfoReply{Error: errorToProto(err)})
	}
	return proto.Marshal(&fabriqpb.BlobInfoReply{Info: objectInfoToProto(info)})
}

// GetBlob is the server side of MethodGetBlob: the first frame carries the object
// metadata (or a setup error), then one data frame per chunk.
func (h *Handler) GetBlob(ctx context.Context, in []byte, send func([]byte) error) error {
	var req fabriqpb.BlobKey
	if err := proto.Unmarshal(in, &req); err != nil {
		return fmt.Errorf("remote: decode get blob request: %w", err)
	}
	rc, info, err := h.fab.Blob().Get(ctx, req.Key)
	if err != nil {
		return sendProto(send, &fabriqpb.GetBlobFrame{Error: errorToProto(err)})
	}
	defer rc.Close()
	if err := sendProto(send, &fabriqpb.GetBlobFrame{Info: objectInfoToProto(info)}); err != nil {
		return err
	}
	buf := make([]byte, blobChunkSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, rerr := rc.Read(buf)
		if n > 0 {
			if err := sendProto(send, &fabriqpb.GetBlobFrame{Data: buf[:n]}); err != nil {
				return err
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}

// HeadBlob, DeleteBlob and PresignBlob are unary.
func (h *Handler) HeadBlob(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.BlobKey
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode head request: %w", err)
	}
	info, err := h.fab.Blob().Head(ctx, req.Key)
	if err != nil {
		return proto.Marshal(&fabriqpb.BlobInfoReply{Error: errorToProto(err)})
	}
	return proto.Marshal(&fabriqpb.BlobInfoReply{Info: objectInfoToProto(info)})
}

func (h *Handler) DeleteBlob(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.BlobKey
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode delete request: %w", err)
	}
	return proto.Marshal(&fabriqpb.BlobAck{Error: errorToProto(h.fab.Blob().Delete(ctx, req.Key))})
}

func (h *Handler) PresignBlob(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.BlobPresign
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode presign request: %w", err)
	}
	ps, ok := h.fab.Blob().(blob.Presigner)
	if !ok {
		return proto.Marshal(&fabriqpb.BlobPresignReply{Error: errorToProto(fmt.Errorf("remote: presign not supported: %w", ErrNotImplemented))})
	}
	ttl := time.Duration(req.TtlSeconds) * time.Second
	var url string
	var err error
	if req.Method == "PUT" {
		url, err = ps.PresignPut(ctx, req.Key, ttl)
	} else {
		url, err = ps.PresignGet(ctx, req.Key, ttl)
	}
	if err != nil {
		return proto.Marshal(&fabriqpb.BlobPresignReply{Error: errorToProto(err)})
	}
	return proto.Marshal(&fabriqpb.BlobPresignReply{Url: url})
}

// ListBlob and CopyBlob are unary.
func (h *Handler) ListBlob(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.ListBlobRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode list blob request: %w", err)
	}
	objs, err := h.fab.Blob().List(ctx, req.Prefix)
	if err != nil {
		return proto.Marshal(&fabriqpb.ListBlobReply{Error: errorToProto(err)})
	}
	pbObjs := make([]*fabriqpb.BlobObjectInfo, len(objs))
	for i, o := range objs {
		pbObjs[i] = objectInfoToProto(o)
	}
	return proto.Marshal(&fabriqpb.ListBlobReply{Objects: pbObjs})
}

func (h *Handler) CopyBlob(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.CopyBlobRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode copy blob request: %w", err)
	}
	info, err := h.fab.Blob().Copy(ctx, req.SrcKey, req.DstKey)
	if err != nil {
		return proto.Marshal(&fabriqpb.BlobInfoReply{Error: errorToProto(err)})
	}
	return proto.Marshal(&fabriqpb.BlobInfoReply{Info: objectInfoToProto(info)})
}

// --- timeseries (query.TSQuerier): telemetry ingest + windowed reads ---

func (h *Handler) TSBulkWrite(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.TSBulkWriteRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode tsBulkWrite request: %w", err)
	}
	var points []query.Point
	if len(req.Points) > 0 {
		if err := json.Unmarshal(req.Points, &points); err != nil {
			return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(fmt.Errorf("remote: decode points: %w", err))})
		}
	}
	return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(h.fab.Timeseries().BulkWrite(ctx, req.Series, points))})
}

func (h *Handler) TSRange(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.TSRangeRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode tsRange request: %w", err)
	}
	var q query.RangeQuery
	if len(req.Query) > 0 {
		if err := json.Unmarshal(req.Query, &q); err != nil {
			return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(fmt.Errorf("remote: decode range query: %w", err))})
		}
	}
	var rows []map[string]any
	if err := h.fab.Timeseries().Range(ctx, q, &rows); err != nil {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(err)})
	}
	row, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("remote: marshal range rows: %w", err)
	}
	return proto.Marshal(&fabriqpb.RowReply{Row: row})
}

// --- spatial (query.SpatialQuerier): geometry storage + radius search ---

func (h *Handler) SpatialUpsert(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.SpatialUpsertRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode spatialUpsert request: %w", err)
	}
	var meta map[string]any
	if len(req.Meta) > 0 {
		if err := json.Unmarshal(req.Meta, &meta); err != nil {
			return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(fmt.Errorf("remote: decode meta: %w", err))})
		}
	}
	geom := query.Geometry{WKT: req.Wkt, SRID: int(req.Srid)}
	return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(h.fab.Spatial().Upsert(ctx, req.Entity, req.Id, geom, meta))})
}

func (h *Handler) SpatialWithin(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.SpatialWithinRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode spatialWithin request: %w", err)
	}
	var q query.SpatialQuery
	if len(req.Query) > 0 {
		if err := json.Unmarshal(req.Query, &q); err != nil {
			return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(fmt.Errorf("remote: decode spatial query: %w", err))})
		}
	}
	var rows []map[string]any
	if err := h.fab.Spatial().Within(ctx, q, &rows); err != nil {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(err)})
	}
	row, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("remote: marshal within rows: %w", err)
	}
	return proto.Marshal(&fabriqpb.RowReply{Row: row})
}

func (h *Handler) SpatialGet(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.SpatialGetRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode spatialGet request: %w", err)
	}
	geom, meta, ok, err := h.fab.Spatial().Get(ctx, req.Entity, req.Id)
	if err != nil {
		return proto.Marshal(&fabriqpb.SpatialGetReply{Error: errorToProto(err)})
	}
	var metaJSON []byte
	if meta != nil {
		if metaJSON, err = json.Marshal(meta); err != nil {
			return nil, fmt.Errorf("remote: marshal spatial meta: %w", err)
		}
	}
	return proto.Marshal(&fabriqpb.SpatialGetReply{Wkt: geom.WKT, Srid: int32(geom.SRID), Meta: metaJSON, Ok: ok})
}

func (h *Handler) SpatialDelete(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.SpatialDeleteRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode spatialDelete request: %w", err)
	}
	return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(h.fab.Spatial().Delete(ctx, req.Entity, req.Id))})
}

func (h *Handler) DocApplyUpdate(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.DocApplyUpdateRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode docApplyUpdate request: %w", err)
	}
	return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(h.fab.Document().ApplyUpdate(ctx, req.DocId, req.Update))})
}

func (h *Handler) DocSync(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.DocSyncRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode docSync request: %w", err)
	}
	upd, err := h.fab.Document().Sync(ctx, req.DocId, req.StateVector)
	if err != nil {
		return proto.Marshal(&fabriqpb.DocSyncReply{Error: errorToProto(err)})
	}
	return proto.Marshal(&fabriqpb.DocSyncReply{Update: upd})
}

func (h *Handler) DocSnapshot(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.DocSnapshotRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode docSnapshot request: %w", err)
	}
	mat, err := h.fab.Document().Snapshot(ctx, req.DocId)
	if err != nil {
		return proto.Marshal(&fabriqpb.RowReply{Error: errorToProto(err)})
	}
	row, err := json.Marshal(mat)
	if err != nil {
		return nil, fmt.Errorf("remote: marshal materialized: %w", err)
	}
	return proto.Marshal(&fabriqpb.RowReply{Row: row})
}

func (h *Handler) DocCompact(ctx context.Context, in []byte) ([]byte, error) {
	var req fabriqpb.DocCompactRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return nil, fmt.Errorf("remote: decode docCompact request: %w", err)
	}
	return proto.Marshal(&fabriqpb.Ack{Error: errorToProto(h.fab.Document().Compact(ctx, req.DocId))})
}

func sendProto(send func([]byte) error, m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return send(b)
}

func (h *Handler) commandFromProto(pc *fabriqpb.Command) (command.Command, error) {
	if pc == nil {
		return command.Command{}, fmt.Errorf("remote: nil command")
	}
	op, err := opFromWire(pc.Op)
	if err != nil {
		return command.Command{}, err
	}
	cmd := command.Command{
		Entity:          pc.Entity,
		Op:              op,
		AggID:           pc.AggId,
		ExpectedVersion: pc.ExpectedVersion,
	}
	if len(pc.Payload) > 0 {
		payload, pErr := h.decodePayload(pc.Entity, pc.Payload)
		if pErr != nil {
			return command.Command{}, pErr
		}
		cmd.Payload = payload
	}
	return cmd, nil
}

// decodePayload turns the opaque JSON body back into the entity's grove model.
// The registry — not the wire schema — is the authority on shape: dynamic
// entities decode to map-native values, static entities to their bound struct
// type. This is the crux of "protobuf envelope, opaque payload" (ADR 0009).
func (h *Handler) decodePayload(entity string, raw []byte) (any, error) {
	ent, ok := h.reg.Get(entity)
	if !ok {
		return nil, fmt.Errorf("remote: unknown entity %q", entity)
	}
	if ent.Binding.IsDynamic() {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("remote: decode dynamic payload for %q: %w", entity, err)
		}
		return m, nil
	}
	model := ent.Binding.NewModel() // pointer to a fresh zero value of the bound type
	if err := json.Unmarshal(raw, model); err != nil {
		return nil, fmt.Errorf("remote: decode payload for %q: %w", entity, err)
	}
	return model, nil
}

// newOne / newMany build the registry-typed scan target the relational port
// fills: static entities scan into their bound struct type (*T / *[]*T),
// dynamic entities into map-native values (parity with the write path).
func newOne(ent *registry.Entity) any {
	if ent.Binding.IsDynamic() {
		return &map[string]any{}
	}
	return ent.Binding.NewModel() // *T
}

func newMany(ent *registry.Entity) any {
	if ent.Binding.IsDynamic() {
		rows := []map[string]any{}
		return &rows
	}
	sliceType := reflect.SliceOf(reflect.PointerTo(ent.Binding.ModelType())) // []*T
	return reflect.New(sliceType).Interface()                                // *[]*T
}
