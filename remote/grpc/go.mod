module github.com/xraph/fabriq/remote/grpc

go 1.26.0

require (
	github.com/xraph/fabriq v0.0.0
	github.com/xraph/grove v1.6.0
	google.golang.org/grpc v1.81.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/oklog/ulid/v2 v2.1.1 // indirect
	github.com/xraph/go-utils v1.1.6 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af // indirect
)

replace github.com/xraph/fabriq => ../..
