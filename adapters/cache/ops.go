package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/xraph/grove/kv"

	corecache "github.com/xraph/fabriq/core/cache"
)

// --- key building -----------------------------------------------------------

func genKey(ks corecache.Keyspace, part string) string {
	return "fabriq:c:gen:" + ks.Name + ":" + part
}

func fullKey(ks corecache.Keyspace, part string, gen int64, key string) string {
	return "fabriq:c:" + ks.Name +
		":v" + strconv.Itoa(ks.Version) +
		":g" + strconv.FormatInt(gen, 10) +
		":" + part + ":" + key
}

// resolve returns (partition segment, current generation) for ks under ctx.
func (a *Adapter) resolve(ctx context.Context, ks corecache.Keyspace) (part string, gen int64, err error) {
	part, err = ks.Partition.Resolve(ctx)
	if err != nil {
		return "", 0, err
	}
	var raw string
	raw, err = a.client.Get(ctx, genKey(ks, part)).Result()
	if errors.Is(err, errRedisNil) {
		return part, 0, nil
	}
	if err != nil {
		return "", 0, fmt.Errorf("fabriq/cache: read generation: %w", err)
	}
	gen, perr := strconv.ParseInt(raw, 10, 64)
	if perr != nil {
		return "", 0, fmt.Errorf("fabriq/cache: parse generation %q: %w", raw, perr)
	}
	return part, gen, nil
}

// --- port methods -----------------------------------------------------------

// Get returns the cached value; ok=false on miss.
func (a *Adapter) Get(ctx context.Context, ks corecache.Keyspace, key string) (val []byte, ok bool, err error) {
	var part string
	var gen int64
	part, gen, err = a.resolve(ctx, ks)
	if err != nil {
		return nil, false, err
	}
	val, err = a.store.GetRaw(ctx, fullKey(ks, part, gen, key))
	if errors.Is(err, kv.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("fabriq/cache: get: %w", err)
	}
	return val, true, nil
}

// Set stores a value under the keyspace policy.
func (a *Adapter) Set(ctx context.Context, ks corecache.Keyspace, key string, val []byte) error {
	part, gen, err := a.resolve(ctx, ks)
	if err != nil {
		return err
	}
	var opts []kv.SetOption
	if ks.Policy.TTL > 0 {
		opts = append(opts, kv.WithTTL(ks.Policy.TTL))
	}
	if err := a.store.SetRaw(ctx, fullKey(ks, part, gen, key), val, opts...); err != nil {
		return fmt.Errorf("fabriq/cache: set: %w", err)
	}
	return nil
}

// GetOrLoad returns the cached value for key, or calls load exactly once
// (single-flight per key) on a miss, stores the result and returns it.
func (a *Adapter) GetOrLoad(ctx context.Context, ks corecache.Keyspace, key string,
	load func(context.Context) ([]byte, error)) ([]byte, error) {
	if v, ok, err := a.Get(ctx, ks, key); err != nil || ok {
		return v, err
	}
	part, gen, err := a.resolve(ctx, ks)
	if err != nil {
		return nil, err
	}
	fk := fullKey(ks, part, gen, key)

	// Single-flight: one loader per key across concurrent callers on this node.
	return a.flight.do(fk, func() ([]byte, error) {
		// Re-check: another flight may have populated it between Get and here.
		if v, ok, err := a.Get(ctx, ks, key); err != nil || ok {
			return v, err
		}
		v, loadErr := load(ctx)
		if loadErr != nil {
			return nil, loadErr
		}
		if err := a.Set(ctx, ks, key, v); err != nil {
			return nil, err
		}
		return v, nil
	})
}

// Invalidate removes specific keys from the keyspace (targeted eviction).
func (a *Adapter) Invalidate(ctx context.Context, ks corecache.Keyspace, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	part, gen, err := a.resolve(ctx, ks)
	if err != nil {
		return err
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = fullKey(ks, part, gen, k)
	}
	if err := a.store.Delete(ctx, full...); err != nil {
		return fmt.Errorf("fabriq/cache: invalidate: %w", err)
	}
	return nil
}

// InvalidateKeyspace bumps the keyspace generation, orphaning every entry
// under it in the current partition at once (no mass deletes).
func (a *Adapter) InvalidateKeyspace(ctx context.Context, ks corecache.Keyspace) error {
	part, err := ks.Partition.Resolve(ctx)
	if err != nil {
		return err
	}
	if err := a.client.Incr(ctx, genKey(ks, part)).Err(); err != nil {
		return fmt.Errorf("fabriq/cache: invalidate keyspace: %w", err)
	}
	return nil
}

// --- single-flight ----------------------------------------------------------

type flightGroup struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

type flightCall struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

func newFlightGroup() *flightGroup { return &flightGroup{m: map[string]*flightCall{}} }

func (g *flightGroup) do(key string, fn func() ([]byte, error)) ([]byte, error) {
	g.mu.Lock()
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &flightCall{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	c.wg.Done()
	return c.val, c.err
}
