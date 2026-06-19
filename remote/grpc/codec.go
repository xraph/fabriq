package remotegrpc

import (
	"fmt"

	"google.golang.org/grpc/encoding"
)

// codecName is the content-subtype identifying the pass-through bytes codec.
const codecName = "fabriq-bytes"

// bytesCodec carries the remote envelope as opaque bytes: the bytes ARE the
// message, exactly as remote.Transport hands them across. This keeps the gRPC
// binding a pure transport — the envelope format (JSON today, protobuf later)
// is owned above the seam, not by gRPC's marshaling.
type bytesCodec struct{}

func (bytesCodec) Name() string { return codecName }

func (bytesCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("remotegrpc: marshal expected []byte, got %T", v)
	}
	return b, nil
}

func (bytesCodec) Unmarshal(data []byte, v any) error {
	p, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("remotegrpc: unmarshal expected *[]byte, got %T", v)
	}
	*p = data
	return nil
}

func init() { encoding.RegisterCodec(bytesCodec{}) }
