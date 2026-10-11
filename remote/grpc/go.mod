module github.com/xraph/fabriq/remote/grpc

go 1.26.0

require (
	github.com/xraph/fabriq v1.7.1
	github.com/xraph/grove v1.7.3
	google.golang.org/grpc v1.83.2
)

require golang.org/x/term v0.46.0 // indirect

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/oklog/ulid/v2 v2.1.2 // indirect
	github.com/xraph/fabriq/core v1.7.1
	github.com/xraph/go-utils v1.3.0 // indirect
	go.opentelemetry.io/otel v1.45.0 // indirect
	go.opentelemetry.io/otel/trace v1.45.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af // indirect
)

replace github.com/xraph/fabriq => ../..

replace github.com/xraph/fabriq/core => ../../core
