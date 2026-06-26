package main

import (
	"context"
	"hash/fnv"
	"math"
	"strings"
	"unicode"
)

// embedderDims is the fixed dimensionality of the demo embedder's output
// vectors. It MUST match the fabriq_embeddings.embedding column, declared
// vector(768) by the postgres schema (migration 0006) — pgvector rejects a row
// whose dimension differs. 768 hashed token buckets are far more than the seed
// corpus needs, so collisions are negligible.
const embedderDims = 768

// localEmbedder is a DETERMINISTIC, NON-SEMANTIC text embedder for the demo.
//
// It is illustrative only — there is NO embedding model behind it. Each input
// string is lowercased and tokenized on non-alphanumeric runes; every token is
// FNV-hashed into one of embedderDims buckets and its bucket weight is summed,
// then the whole vector is L2-normalized. The consequences are exactly what a
// vector-search demo needs:
//
//   - the SAME text always yields the SAME vector (deterministic, so repeated
//     seeding is idempotent and similar-to-entity is stable);
//   - texts that SHARE tokens land closer in cosine space than texts that share
//     none (so "widget" matches rows whose name/sku contain "widget"-ish
//     tokens) — purely lexical overlap, not meaning.
//
// It implements agent.Embedder so it can be passed to adminapi.WithEmbedder and
// used to embed seeded rows. Do NOT use it for anything but a local demo.
type localEmbedder struct{}

// newLocalEmbedder returns the demo embedder.
func newLocalEmbedder() localEmbedder { return localEmbedder{} }

// Dims reports the embedding dimensionality (always embedderDims).
func (localEmbedder) Dims() int { return embedderDims }

// Embed returns one L2-normalized vector per input string, in order. It never
// returns an error: hashing is total over any UTF-8 string.
func (e localEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = e.vector(t)
	}
	return out, nil
}

// vector hashes the tokens of s into a fixed-dimension bucket-sum vector and
// L2-normalizes it. An empty/token-less input yields a zero vector (cosine
// similarity is then 0 against everything, which is the desired "no signal").
func (localEmbedder) vector(s string) []float32 {
	v := make([]float32, embedderDims)
	for _, tok := range tokenize(s) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(tok))
		sum := h.Sum32()
		bucket := int(sum % embedderDims)
		// Sign from a high bit so distinct tokens can cancel/reinforce rather
		// than every token only ever adding positive mass.
		if sum&0x80000000 != 0 {
			v[bucket] -= 1
		} else {
			v[bucket] += 1
		}
	}
	return l2normalize(v)
}

// tokenize lowercases s and splits it on any non-letter/non-digit rune,
// dropping empty tokens. SKU separators like '-' therefore split "ACME-SKU-0001"
// into ["acme", "sku", "0001"].
func tokenize(s string) []string {
	fields := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	return fields
}

// l2normalize scales v to unit length in place and returns it. A zero vector is
// returned unchanged (avoids a divide-by-zero).
func l2normalize(v []float32) []float32 {
	var sumSq float64
	for _, x := range v {
		sumSq += float64(x) * float64(x)
	}
	if sumSq == 0 {
		return v
	}
	inv := float32(1.0 / math.Sqrt(sumSq))
	for i := range v {
		v[i] *= inv
	}
	return v
}
