package cache

import "encoding/json"

// Codec serializes typed values for Typed[T]. The byte-level Cache port is
// codec-agnostic; Typed[T] applies a codec at the edge.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, into any) error
}

// JSON is the default codec.
type JSON struct{}

func (JSON) Encode(v any) ([]byte, error)       { return json.Marshal(v) }
func (JSON) Decode(data []byte, into any) error { return json.Unmarshal(data, into) }
