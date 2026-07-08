// Package remote is the OPTIONAL server face of fabriq: it lets backend
// services talk to a central, connection-owning fabriq deployment over gRPC
// instead of embedding the library and owning their own datastore pools. It
// changes the DEPLOYMENT TOPOLOGY, not the engine — Fabric implements the
// same core/query.Fabric interface application code already holds, so the call
// sites are identical (ADR 0009).
//
// Layering. The client (Fabric) and server (Handler) sit on a narrow,
// codec-neutral Transport seam so the request/response envelope can be
// exercised — and unit-tested via Loopback — before the gRPC+protobuf binding
// (proto/fabriq/v1/fabriq.proto) is generated and wired. The production
// Transport is gRPC over HTTP/2 with mTLS; tenant and principal travel in call
// metadata, authenticated at the server edge, never trusted from a client
// field.
//
// Scope. The write plane (Exec/ExecBatch), the relational and projection read
// ports (Get/GetMany/List, Graph/Search/Vector), the Timeseries and Spatial
// ports, the live plane (Subscribe plus maintained LiveQuery with Reanchor over
// a bidirectional stream), the blob byte plane (Put/Get/Head/Delete/List/Copy
// and the presign bypass) and the Document plane are all wired end-to-end, with
// registry-typed payload decoding and the typed-error taxonomy across the wire.
// Interactive (multi-round-trip) transactions remain a deliberate non-goal;
// raw-SQL Query, blob multipart/range, and the optional document history
// sub-interfaces are the remaining follow-ons (their accessors return
// ErrNotImplemented).
package remote
