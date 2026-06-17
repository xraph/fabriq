package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Fingerprint is a stable content hash of a normalized query value (entity,
// filter, sort, limit, offset, or traversal spec). It is the cache key for
// query keyspaces. encoding/json sorts map keys, so logically-equal queries
// that differ only in map literal order produce the same fingerprint.
// Partition is NOT part of the fingerprint — it comes from the keyspace+ctx,
// so the same filter never collides across tenants or scopes.
func Fingerprint(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("fabriq/cache: fingerprint: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16]), nil // 128-bit hex; collision-safe for keys
}
