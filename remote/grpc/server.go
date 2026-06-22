package remotegrpc

import (
	"context"

	"google.golang.org/grpc"

	"github.com/xraph/fabriq/remote"
)

// fabriqService is the registration marker for the gRPC service. The handlers
// dispatch by method name into remote.Handler, so no generated service
// interface is needed.
type fabriqService interface{}

// server adapts a *remote.Handler to the gRPC service handlers.
type server struct{ h *remote.Handler }

// Register registers the fabriq.v1.Fabriq service, backed by h, on s. The
// caller owns the *grpc.Server — its TLS credentials, interceptors (where the
// tenant/principal are authenticated from call metadata) and listener.
func Register(s grpc.ServiceRegistrar, h *remote.Handler) {
	s.RegisterService(&serviceDesc, &server{h: h})
}

func unaryHandler(method string) func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		var in []byte
		if err := dec(&in); err != nil {
			return nil, err
		}
		// invoke is the real handler. When the server has a (chained) unary
		// interceptor, it MUST be called around invoke — exactly as generated
		// stubs do — or interceptors (auth, etc.) silently never run.
		invoke := func(ctx context.Context, req any) (any, error) {
			return srv.(*server).h.Dispatch(ctx, method, req.([]byte))
		}
		if interceptor == nil {
			return invoke(ctx, in)
		}
		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + method}
		return interceptor(ctx, in, info, invoke)
	}
}

// streamHandler builds a server-streaming gRPC handler that reads the single
// request frame and pipes the call into remote.Handler.DispatchStream under the
// given method name.
func streamHandler(method string) func(any, grpc.ServerStream) error {
	return func(srv any, stream grpc.ServerStream) error {
		var in []byte
		if err := stream.RecvMsg(&in); err != nil {
			return err
		}
		send := func(b []byte) error { return stream.SendMsg(b) }
		return srv.(*server).h.DispatchStream(stream.Context(), method, in, send)
	}
}

// clientStreamHandler builds a client-streaming gRPC handler that pipes received
// frames into remote.Handler.DispatchClientStream and sends back its single
// reply.
func clientStreamHandler(method string) func(any, grpc.ServerStream) error {
	return func(srv any, stream grpc.ServerStream) error {
		recv := func() ([]byte, error) {
			var b []byte
			if err := stream.RecvMsg(&b); err != nil {
				return nil, err // io.EOF when the client CloseSends
			}
			return b, nil
		}
		reply, err := srv.(*server).h.DispatchClientStream(stream.Context(), method, recv)
		if err != nil {
			return err
		}
		return stream.SendMsg(reply)
	}
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "fabriq.v1.Fabriq",
	HandlerType: (*fabriqService)(nil),
	// gRPC routes by this table, so every new unary method must be enumerated
	// here as well as in remote.Handler.Dispatch — otherwise it is reachable
	// over Loopback but Unimplemented over gRPC.
	Methods: []grpc.MethodDesc{
		{MethodName: "Exec", Handler: unaryHandler(remote.MethodExec)},
		{MethodName: "ExecBatch", Handler: unaryHandler(remote.MethodExecBatch)},
		{MethodName: "Get", Handler: unaryHandler(remote.MethodGet)},
		{MethodName: "GetMany", Handler: unaryHandler(remote.MethodGetMany)},
		{MethodName: "List", Handler: unaryHandler(remote.MethodList)},
		{MethodName: "HeadBlob", Handler: unaryHandler(remote.MethodHeadBlob)},
		{MethodName: "DeleteBlob", Handler: unaryHandler(remote.MethodDeleteBlob)},
		{MethodName: "PresignBlob", Handler: unaryHandler(remote.MethodPresignBlob)},
		{MethodName: "VectorSimilar", Handler: unaryHandler(remote.MethodVectorSimilar)},
		{MethodName: "VectorUpsert", Handler: unaryHandler(remote.MethodVectorUpsert)},
		{MethodName: "VectorDelete", Handler: unaryHandler(remote.MethodVectorDelete)},
		{MethodName: "VectorDeleteByMeta", Handler: unaryHandler(remote.MethodVectorDeleteByMeta)},
		{MethodName: "VectorGet", Handler: unaryHandler(remote.MethodVectorGet)},
		{MethodName: "Search", Handler: unaryHandler(remote.MethodSearch)},
		{MethodName: "GraphQuery", Handler: unaryHandler(remote.MethodGraphQuery)},
	},
	// gRPC routes by this table; every new streaming method must be enumerated
	// here as well as in remote.Handler.DispatchStream / DispatchClientStream.
	Streams: []grpc.StreamDesc{
		{StreamName: "Subscribe", Handler: streamHandler(remote.MethodSubscribe), ServerStreams: true},
		{StreamName: "LiveQuery", Handler: streamHandler(remote.MethodLiveQuery), ServerStreams: true},
		{StreamName: "GetBlob", Handler: streamHandler(remote.MethodGetBlob), ServerStreams: true},
		{StreamName: "PutBlob", Handler: clientStreamHandler(remote.MethodPutBlob), ClientStreams: true},
	},
	Metadata: "remote/proto/fabriq/v1/fabriq.proto",
}
