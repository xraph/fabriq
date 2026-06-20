package agent

import "context"

// GuardStage marks which stage of distillation a guard call protects.
type GuardStage int

const (
	// GuardIngest redacts raw content BEFORE the Summarizer sees it.
	GuardIngest GuardStage = iota
	// GuardEmit checks a generated summary BEFORE it is hashed + written to CAS.
	GuardEmit
)

// GuardInput is one guard call.
type GuardInput struct {
	Stage    GuardStage `json:"stage"`
	TenantID string     `json:"tenantId"`
	Scope    ScopeRef   `json:"scope"`
	Level    int        `json:"level"`
	Text     string     `json:"text"`
}

// GuardResult is a guard verdict: possibly-redacted text, a block flag, and an
// audit reason.
type GuardResult struct {
	Text    string `json:"text"`
	Blocked bool   `json:"blocked"`
	Reason  string `json:"reason"`
}

// Guard is the host-supplied, optional PII/guardrail seam (nil = identity).
type Guard interface {
	Guard(ctx context.Context, in GuardInput) (GuardResult, error)
}

// applyGuard runs the guard with the deployment's failure policy. A nil guard is
// identity (pass-through). On a guard error: fail-closed (Blocked=true) by
// default, or fail-open (pass the original text) when failOpen is set. It never
// returns an error — the policy is encoded in the result, so callers branch on
// Blocked, not err.
func applyGuard(ctx context.Context, g Guard, in GuardInput, failOpen bool) GuardResult {
	if g == nil {
		return GuardResult{Text: in.Text}
	}
	r, err := g.Guard(ctx, in)
	if err != nil {
		if failOpen {
			return GuardResult{Text: in.Text}
		}
		return GuardResult{Blocked: true, Reason: "guard error (fail-closed): " + err.Error()}
	}
	if r.Text == "" && !r.Blocked {
		r.Text = in.Text // a guard that redacts nothing returns the input
	}
	return r
}
