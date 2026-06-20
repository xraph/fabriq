package shieldguard

import (
	"context"
	"testing"

	"github.com/xraph/shield/scan"

	"github.com/xraph/fabriq/core/agent"
)

type fakeScanner struct{ in, out *scan.Result }

func (f fakeScanner) ScanInput(context.Context, *scan.Input) (*scan.Result, error) { return f.in, nil }
func (f fakeScanner) ScanOutput(context.Context, *scan.Input) (*scan.Result, error) {
	return f.out, nil
}

func TestGuard_RedactOnIngest(t *testing.T) {
	g := New(fakeScanner{in: &scan.Result{Decision: scan.DecisionRedact, Redacted: "[REDACTED]"}})
	r, err := g.Guard(context.Background(), agent.GuardInput{Stage: agent.GuardIngest, Text: "ssn 123"})
	if err != nil || r.Text != "[REDACTED]" {
		t.Fatalf("expected redacted text, got %+v err=%v", r, err)
	}
}

func TestGuard_BlockOnEmit(t *testing.T) {
	g := New(fakeScanner{out: &scan.Result{Decision: scan.DecisionBlock, Blocked: true}})
	r, _ := g.Guard(context.Background(), agent.GuardInput{Stage: agent.GuardEmit, Text: "secret"})
	if !r.Blocked {
		t.Fatal("expected blocked on emit")
	}
}
