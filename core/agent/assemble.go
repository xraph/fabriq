package agent

import (
	"encoding/json"
	"sort"
)

// ref identifies a candidate row across channels.
type ref struct {
	Entity string
	ID     string
}

// scoredRef is a fused candidate with its RRF score and contributing channels.
type scoredRef struct {
	ref
	score   float64
	sources []string
}

// ContextItem is one hydrated row in a recall result.
type ContextItem struct {
	Entity    string          `json:"entity"`
	ID        string          `json:"id"`
	Row       json.RawMessage `json:"row"`
	Score     float64         `json:"score"`
	Source    []string        `json:"source"`
	Tokens    int             `json:"tokens"`
	Bucket    uint64          `json:"bucket,omitempty"` // entity's L0-digest SemHash, for cluster-coverage dedup
	BucketSet bool            `json:"-"`                // true only when Bucket was populated by a successful L0-digest lookup
}

// ContextPack is the token-budgeted recall result.
type ContextPack struct {
	Items    []ContextItem `json:"items"`
	Omitted  int           `json:"omitted"`
	Tokens   int           `json:"tokens"`
	Warnings []string      `json:"warnings,omitempty"`
}

// rrfK is the Reciprocal Rank Fusion damping constant (standard value 60).
const rrfK = 60

// fuse combines per-channel ranked candidate lists into one ranked list by
// Reciprocal Rank Fusion. channels maps a channel name to its candidates in
// best-first order. weights scales each channel (missing/non-positive → 1).
// Result is sorted by descending score with a deterministic (entity,id) tiebreak.
func fuse(channels map[string][]ref, weights map[string]float64) []scoredRef {
	score := map[ref]float64{}
	srcSet := map[ref]map[string]struct{}{}
	for name, refs := range channels {
		w := weights[name]
		if w <= 0 {
			w = 1
		}
		for rank, r := range refs {
			score[r] += w / float64(rrfK+rank+1) // rank is 0-based → 1-based
			if srcSet[r] == nil {
				srcSet[r] = map[string]struct{}{}
			}
			srcSet[r][name] = struct{}{}
		}
	}
	out := make([]scoredRef, 0, len(score))
	for r, s := range score {
		out = append(out, scoredRef{ref: r, score: s, sources: sortedKeys(srcSet[r])})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].score != out[j].score {
			return out[i].score > out[j].score
		}
		if out[i].Entity != out[j].Entity {
			return out[i].Entity < out[j].Entity
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func sortedKeys(m map[string]struct{}) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// pack greedily includes items in order until the next would exceed budget,
// then stops; the rest are counted as omitted. Non-positive budget keeps none.
func pack(items []ContextItem, budget int) (kept []ContextItem, omitted, used int) {
	for i := range items {
		if used+items[i].Tokens > budget {
			return kept, len(items) - i, used
		}
		kept = append(kept, items[i])
		used += items[i].Tokens
	}
	return kept, 0, used
}
