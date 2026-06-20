package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

const hashSep = "\x00"

// SourceFieldHash hashes an L0 node's source text (the concatenated fields).
func SourceFieldHash(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// L0ContentHash is the Merkle hash of a leaf: h(recipeVersion ‖ sourceFieldHash).
// It is structural (over the source, not the non-deterministic summary text) and
// gates whether the Summarizer is called at all.
func L0ContentHash(recipeVersion, sourceFieldHash string) string {
	return hashJoin(recipeVersion, sourceFieldHash)
}

// RollupContentHash is the Merkle hash of an internal node:
// h(recipeVersion ‖ sorted child ContentHashes). Sorting makes it independent
// of child order; any child change propagates up.
func RollupContentHash(recipeVersion string, childHashes []string) string {
	sorted := append([]string(nil), childHashes...)
	sort.Strings(sorted)
	return hashJoin(append([]string{recipeVersion}, sorted...)...)
}

func hashJoin(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, hashSep)))
	return hex.EncodeToString(sum[:])
}
