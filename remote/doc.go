// Package remote is the OPTIONAL server face of fabriq: it lets backend
// services talk to a central, connection-owning fabriq deployment over gRPC
// instead of embedding the library and owning their own datastore pools. It
// changes the DEPLOYMENT TOPOLOGY, not the engine — RemoteFabric implements the
// same core/query.Fabric interface application code already holds, so the call
// sites are identical (ADR 0009).
//
// Layering. The client (RemoteFabric) and server (Handler) sit on a narrow,
// codec-neutral Transport seam so the request/response envelope can be
// exercised — and unit-tested via Loopback — before the gRPC+protobuf binding
// (proto/fabriq/v1/fabriq.proto) is generated and wired. The production
// Transport is gRPC over HTTP/2 with mTLS; tenant and principal travel in call
// metadata, authenticated at the server edge, never trusted from a client
// field.
//
// Scope of this skeleton. The write plane (Exec/ExecBatch) is implemented
// end-to-end to pressure-test the envelope, including registry-typed payload
// decoding on the server and the typed-error taxonomy across the wire. The
// read, live, blob and interactive-transaction planes named in the proto are
// follow-ons; their accessors return ErrNotImplemented.
package remote
