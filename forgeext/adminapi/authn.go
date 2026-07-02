package adminapi

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
)

// base62Alphabet is used to render random key bytes into a URL-safe,
// unambiguous character set for API keys.
const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// generateKey creates a new random API key of the form "fq_<base62>", along
// with its lookup prefix (the first 7 characters of the key, used for
// non-secret indexed lookup) and its sha256 hex digest (used for storage and
// verification without persisting the raw key).
func generateKey() (key, prefix, hash string, err error) {
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return "", "", "", err
	}

	encoded := make([]byte, len(raw))
	for i, b := range raw {
		encoded[i] = base62Alphabet[int(b)%len(base62Alphabet)]
	}

	key = "fq_" + string(encoded)
	prefix = key[:7]
	hash = hashKey(key)
	return key, prefix, hash, nil
}

// hashKey returns the sha256 hex digest of key, used as the stored/lookup
// representation so raw keys are never persisted.
func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// constantTimeEqualHash compares two hash strings in constant time to avoid
// timing side-channels during key verification.
func constantTimeEqualHash(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
