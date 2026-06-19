package remote

// Regenerate the wire-envelope types from the .proto. Messages only — no gRPC
// service stub — so this package gains a protobuf dependency but never gRPC; the
// gRPC binding lives in the separate remote/grpc module. Run `go generate ./...`
// with protoc + protoc-gen-go on PATH.
//
//go:generate protoc -I proto --go_out=.. --go_opt=module=github.com/xraph/fabriq proto/fabriq/v1/fabriq.proto
