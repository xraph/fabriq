package remotegrpc

import (
	"google.golang.org/grpc"

	"github.com/xraph/fabriq/remote"
)

// bidiEchoStream is the descriptor for the diagnostic BidiEcho method. It lives
// in a _test.go file so the bidi primitive can be exercised over real gRPC
// without BidiEcho ever appearing in the production serviceDesc (server.go).
var bidiEchoStream = grpc.StreamDesc{
	StreamName:    "BidiEcho",
	Handler:       bidiHandler(remote.MethodBidiEcho),
	ClientStreams: true,
	ServerStreams: true,
}

// RegisterWithBidiEcho registers the fabriq.v1.Fabriq service backed by h, plus
// the diagnostic BidiEcho stream. It is the test-only counterpart to Register:
// the F1 bidi tests use it to reach BidiEcho over gRPC, while production code
// (Register) leaves the diagnostic method off the shipped service surface.
func RegisterWithBidiEcho(s grpc.ServiceRegistrar, h *remote.Handler) {
	desc := serviceDesc // shallow copy of the production descriptor
	// Fresh backing array so we never mutate serviceDesc.Streams in place.
	desc.Streams = append(append([]grpc.StreamDesc{}, serviceDesc.Streams...), bidiEchoStream)
	s.RegisterService(&desc, &server{h: h})
}
