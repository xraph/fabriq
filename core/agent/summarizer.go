package agent

import "context"

// ScopeRef names a scope a digest summarizes (empty for tenant/cluster nodes).
type ScopeRef struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

// ChildDigest is a child summary fed into a rollup summarization.
type ChildDigest struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Summary string `json:"summary"`
}

// SummaryInput is the host-model input for one summarization. For L0, Raw holds
// the source text; for L1/L2, Children holds the child summaries.
type SummaryInput struct {
	Level    int           `json:"level"`
	Kind     string        `json:"kind"`
	Scope    ScopeRef      `json:"scope"`
	Children []ChildDigest `json:"children"`
	Raw      []byte        `json:"raw"`
	Budget   int           `json:"budget"`
}

// Summarizer is the host-supplied summarization seam. fabriq stays model-agnostic.
type Summarizer interface {
	Summarize(ctx context.Context, in SummaryInput) (string, error)
}
