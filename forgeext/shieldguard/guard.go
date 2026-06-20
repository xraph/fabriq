// Package shieldguard adapts github.com/xraph/shield to the agent.Guard seam.
// It is opt-in: core/agent never imports shield. Wire it with
// forgeext.WithGuard(shieldguard.New(shieldExt.Engine())).
package shieldguard

import (
	"context"
	"strings"

	"github.com/xraph/shield"
	"github.com/xraph/shield/scan"

	"github.com/xraph/fabriq/core/agent"
)

// Scanner is the subset of *engine.Engine this adapter needs (so it is testable
// without standing up a full shield engine).
type Scanner interface {
	ScanInput(ctx context.Context, in *scan.Input) (*scan.Result, error)
	ScanOutput(ctx context.Context, in *scan.Input) (*scan.Result, error)
}

// Guard adapts a shield Scanner to agent.Guard.
type Guard struct{ s Scanner }

// New builds a Guard over a shield Scanner (an *engine.Engine satisfies Scanner).
func New(s Scanner) agent.Guard { return Guard{s: s} }

// Guard scans in.Text through the shield engine and maps the verdict onto
// agent.GuardResult. GuardIngest scans input (user->agent), GuardEmit scans
// output (agent->user). On a scanner error it returns the error unmodified so
// the caller's applyGuard can apply the deployment's fail-closed/open policy.
func (g Guard) Guard(ctx context.Context, in agent.GuardInput) (agent.GuardResult, error) {
	ctx = shield.WithTenant(ctx, in.TenantID)
	scanFn := g.s.ScanInput
	if in.Stage == agent.GuardEmit {
		scanFn = g.s.ScanOutput
	}
	r, err := scanFn(ctx, &scan.Input{Text: in.Text})
	if err != nil {
		return agent.GuardResult{}, err // caller applies fail-closed policy
	}
	out := agent.GuardResult{Text: in.Text, Blocked: r.Blocked}
	if r.Decision == scan.DecisionRedact && r.Redacted != "" {
		out.Text = r.Redacted
	}
	if r.HasFindings() {
		out.Reason = summarizeFindings(r.Findings)
	}
	return out, nil
}

func summarizeFindings(fs []*scan.Finding) string {
	parts := make([]string, 0, len(fs))
	for _, f := range fs {
		parts = append(parts, f.Layer+":"+f.Message)
	}
	return strings.Join(parts, "; ")
}
