package agent

import (
	"context"
	"regexp"
	"strings"

	"github.com/xraph/grove"
)

// fakeSummarizer is deterministic: it concatenates child summaries or the raw
// text and truncates to Budget words. No model call.
type fakeSummarizer struct{ calls int }

func (f *fakeSummarizer) Summarize(_ context.Context, in SummaryInput) (string, error) {
	f.calls++
	var src string
	if len(in.Children) > 0 {
		parts := make([]string, len(in.Children))
		for i, c := range in.Children {
			parts[i] = c.Summary
		}
		src = "rollup(" + strings.Join(parts, "|") + ")"
	} else {
		src = "l0(" + string(in.Raw) + ")"
	}
	words := strings.Fields(src)
	if in.Budget > 0 && len(words) > in.Budget {
		words = words[:in.Budget]
	}
	return strings.Join(words, " "), nil
}

// fakeGuard redacts a fixed PII pattern (digit runs) and blocks text containing
// "SECRET".
type fakeGuard struct{ re *regexp.Regexp }

func newFakeGuard() *fakeGuard { return &fakeGuard{re: regexp.MustCompile(`\d{3,}`)} }

func (g *fakeGuard) Guard(_ context.Context, in GuardInput) (GuardResult, error) {
	if strings.Contains(in.Text, "SECRET") {
		return GuardResult{Blocked: true, Reason: "contains SECRET"}, nil
	}
	return GuardResult{Text: g.re.ReplaceAllString(in.Text, "[REDACTED]")}, nil
}

// digestModel is a test-local grove model matching the digest_nodes columns so
// the fake relational store can persist digest rows written via the command
// plane. It mirrors domain.DigestNode without importing domain (import-cleanliness).
type digestModel struct {
	grove.BaseModel `grove:"table:digest_nodes"`

	ID          string   `grove:"id,pk"`
	TenantID    string   `grove:"tenant_id,notnull"`
	Version     int64    `grove:"version,notnull"`
	Level       int      `grove:"level,notnull"`
	Kind        string   `grove:"kind,notnull"`
	ScopeName   string   `grove:"scope_name"`
	ScopeID     string   `grove:"scope_id"`
	SourceID    string   `grove:"source_id"`
	SourceKind  string   `grove:"source_kind"`
	SummaryHash string   `grove:"summary_hash"`
	ContentHash string   `grove:"content_hash"`
	SemHash     string   `grove:"sem_hash"`
	ChildIDs    []string `grove:"child_ids"`
	ParentIDs   []string `grove:"parent_ids"`
	UpdatedAt   int64    `grove:"updated_at"`
}
