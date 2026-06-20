package agent

import (
	"context"
	"errors"
	"testing"
)

type denyGuard struct{}

func (denyGuard) Guard(context.Context, GuardInput) (GuardResult, error) {
	return GuardResult{}, errors.New("guard down")
}

func TestApplyGuard_NilIsIdentity(t *testing.T) {
	r := applyGuard(context.Background(), nil, GuardInput{Text: "abc"}, false)
	if r.Blocked || r.Text != "abc" {
		t.Fatalf("nil guard must pass through: %+v", r)
	}
}

func TestApplyGuard_FailClosedOnError(t *testing.T) {
	r := applyGuard(context.Background(), denyGuard{}, GuardInput{Text: "abc"}, false)
	if !r.Blocked {
		t.Fatal("fail-closed: a guard error must block the content")
	}
}

func TestApplyGuard_FailOpenOnError(t *testing.T) {
	r := applyGuard(context.Background(), denyGuard{}, GuardInput{Text: "abc"}, true)
	if r.Blocked || r.Text != "abc" {
		t.Fatalf("fail-open must pass original text through: %+v", r)
	}
}
