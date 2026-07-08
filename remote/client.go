package remote

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// Fabric is the client face: it implements core/query.Fabric by
// marshaling each call onto a Transport. Application code holds it exactly as it
// holds the embedded *fabriq.Fabriq — same interface, same call sites
// (ADR 0009). For typed repositories use query.For[T](reg, f.Relational())
// rather than fabriq.For[T], which is bound to the concrete embedded facade.
type Fabric struct {
	t Transport
}

// New builds a Fabric over a Transport (gRPC in production, Loopback in
// tests).
func New(t Transport) *Fabric {
	return &Fabric{t: t}
}

var _ query.Fabric = (*Fabric)(nil)

// Exec sends one command and reconstructs the result (or typed error).
func (r *Fabric) Exec(ctx context.Context, cmd command.Command) (command.Result, error) {
	pc, err := commandToProto(cmd)
	if err != nil {
		return command.Result{}, err
	}
	in, err := proto.Marshal(&fabriqpb.ExecRequest{Command: pc})
	if err != nil {
		return command.Result{}, err
	}
	out, err := r.t.Unary(ctx, MethodExec, in)
	if err != nil {
		return command.Result{}, err
	}
	var reply fabriqpb.ExecReply
	if err := proto.Unmarshal(out, &reply); err != nil {
		return command.Result{}, fmt.Errorf("remote: decode exec reply: %w", err)
	}
	if reply.Error != nil {
		return command.Result{}, errorFromProto(reply.Error)
	}
	return resultFromProto(reply.Result), nil
}

// ExecBatch sends N commands to run in one server-side transaction.
func (r *Fabric) ExecBatch(ctx context.Context, cmds []command.Command) ([]command.Result, error) {
	pcs := make([]*fabriqpb.Command, len(cmds))
	for i, c := range cmds {
		pc, err := commandToProto(c)
		if err != nil {
			return nil, err
		}
		pcs[i] = pc
	}
	in, err := proto.Marshal(&fabriqpb.ExecBatchRequest{Commands: pcs})
	if err != nil {
		return nil, err
	}
	out, err := r.t.Unary(ctx, MethodExecBatch, in)
	if err != nil {
		return nil, err
	}
	var reply fabriqpb.ExecBatchReply
	if err := proto.Unmarshal(out, &reply); err != nil {
		return nil, fmt.Errorf("remote: decode execBatch reply: %w", err)
	}
	if reply.Error != nil {
		return nil, errorFromProto(reply.Error)
	}
	results := make([]command.Result, len(reply.Results))
	for i, w := range reply.Results {
		results[i] = resultFromProto(w)
	}
	return results, nil
}

// --- read / stream planes. Relational Get/GetMany/List are wired over the unary
// transport (relational.go); the other projection ports, the live plane and the
// blob plane are follow-ons whose calls return ErrNotImplemented until they
// land (ADR 0009 sequencing). ---

func (r *Fabric) Relational() query.RelationalQuerier { return remoteRelational{t: r.t} }
func (r *Fabric) Graph() query.GraphQuerier           { return remoteGraph{t: r.t} }
func (r *Fabric) Search() query.SearchQuerier         { return remoteSearch{t: r.t} }
func (r *Fabric) Timeseries() query.TSQuerier         { return remoteTS{t: r.t} }
func (r *Fabric) Vector() query.VectorQuerier         { return remoteVector{t: r.t} }
func (r *Fabric) Spatial() query.SpatialQuerier       { return remoteSpatial{t: r.t} }

// Document returns nil until the document plane is wired. Blob streams bytes
// (Put/Get) and the presign bypass over the transport; List/Copy are follow-ons.
func (r *Fabric) Document() document.Store { return nil }
func (r *Fabric) Blob() blob.Store         { return remoteBlobStore{t: r.t} }

// Subscribe opens the conflated channel-delta stream. The first frame is a
// handshake: a setup error (authz / scope resolution) returns synchronously,
// mirroring the in-process contract; otherwise a goroutine drains delta frames
// into the returned channel until the stream ends or ctx is cancelled.
func (r *Fabric) Subscribe(ctx context.Context, scope query.SubscribeScope) (<-chan query.Delta, error) {
	in, err := proto.Marshal(&fabriqpb.SubscribeRequest{Scope: scopeToProto(scope)})
	if err != nil {
		return nil, err
	}
	stream, err := r.t.ServerStream(ctx, MethodSubscribe, in)
	if err != nil {
		return nil, err
	}
	first, err := stream.Recv()
	if err != nil {
		_ = stream.Close()
		return nil, err
	}
	var hs fabriqpb.SubFrame
	if err := proto.Unmarshal(first, &hs); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("remote: decode subscribe handshake: %w", err)
	}
	if hs.Error != nil {
		_ = stream.Close()
		return nil, errorFromProto(hs.Error)
	}
	out := make(chan query.Delta)
	go func() {
		defer close(out)
		defer stream.Close()
		for {
			frame, rerr := stream.Recv()
			if rerr != nil {
				return // io.EOF (clean end) or transport error
			}
			var sf fabriqpb.SubFrame
			if proto.Unmarshal(frame, &sf) != nil || sf.Delta == nil {
				continue
			}
			select {
			case out <- deltaFromProto(sf.Delta):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// WaitForProjection is a read-your-writes helper; it rides the read plane.
func (r *Fabric) WaitForProjection(_ context.Context, _, _, _ string, _ int64) error {
	return ErrNotImplemented
}
