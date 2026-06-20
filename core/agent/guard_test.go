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
	r, err := applyGuard(context.Background(), nil, GuardInput{Text: "abc"}, false)
	if err != nil || r.Blocked || r.Text != "abc" {
		t.Fatalf("nil guard must pass through: %+v err=%v", r, err)
	}
}

func TestApplyGuard_FailClosedOnError(t *testing.T) {
	r, err := applyGuard(context.Background(), denyGuard{}, GuardInput{Text: "abc"}, false)
	if err != nil {
		t.Fatalf("fail-closed must not surface error to caller as failure: %v", err)
	}
	if !r.Blocked {
		t.Fatal("fail-closed: a guard error must block the content")
	}
}

func TestApplyGuard_FailOpenOnError(t *testing.T) {
	r, err := applyGuard(context.Background(), denyGuard{}, GuardInput{Text: "abc"}, true)
	if err != nil || r.Blocked || r.Text != "abc" {
		t.Fatalf("fail-open must pass original text through: %+v err=%v", r, err)
	}
}
