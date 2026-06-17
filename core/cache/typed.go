package cache

import "context"

// Typed[T] is a type-safe view over a Cache + Keyspace, applying a Codec at
// the edge so callers work with T, not bytes. core stays codec-agnostic; the
// byte-level Cache is the transport.
type Typed[T any] struct {
	c     Cache
	ks    Keyspace
	codec Codec
}

// For builds a Typed[T] using the default JSON codec.
func For[T any](c Cache, ks Keyspace) Typed[T] {
	return Typed[T]{c: c, ks: ks, codec: JSON{}}
}

// GetOrLoad returns the cached T, or calls load once on a miss, encodes and
// stores the result, and returns it.
func (t Typed[T]) GetOrLoad(ctx context.Context, key string,
	load func(context.Context) (T, error)) (T, error) {
	var zero T
	raw, err := t.c.GetOrLoad(ctx, t.ks, key, func(ctx context.Context) ([]byte, error) {
		v, err := load(ctx)
		if err != nil {
			return nil, err
		}
		return t.codec.Encode(v)
	})
	if err != nil {
		return zero, err
	}
	var out T
	if err := t.codec.Decode(raw, &out); err != nil {
		return zero, err
	}
	return out, nil
}
