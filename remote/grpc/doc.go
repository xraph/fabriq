// Package remotegrpc is the gRPC binding for the fabriq remote protocol — the
// real network transport that replaces remote.Loopback. It lives in its own Go
// module so google.golang.org/grpc and its dependency tree stay OUT of the core
// github.com/xraph/fabriq module: services that only embed the library never
// pull gRPC transitively (ADR 0009; the dependency-fencing discipline of
// ADR 0008).
//
// Client implements remote.Transport over a *grpc.ClientConn; Register adapts a
// *remote.Handler onto a *grpc.Server. Both sides select a pass-through bytes
// codec, so the remote envelope crosses gRPC untouched — gRPC supplies HTTP/2
// framing, multiplexing, deadlines and (via the caller's transport credentials)
// mTLS, while the envelope format stays owned above the seam.
//
// NOTE: this first binding frames the JSON envelope that remote produces today.
// Migrating the envelope to the typed protobuf messages in
// remote/proto/fabriq/v1/fabriq.proto is a follow-on that does not touch
// remote.Handler.
package remotegrpc
