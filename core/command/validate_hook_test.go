package command_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
)

type hookModel struct {
	grove.BaseModel `grove:"table:widgets"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Kind            string `grove:"kind,notnull"`
}

func TestExec_ValidateHookRejects(t *testing.T) {
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "widget", Kind: registry.KindAggregate, Model: (*hookModel)(nil), GraphNode: "Widget",
		Validate: func(vals map[string]any) error {
			if vals["kind"] == "bad" {
				return errors.New("kind not allowed")
			}
			return nil
		},
	})
	x, err := command.NewExecutor(r, newFakeStore())
	if err != nil {
		t.Fatal(err)
	}
	_, err = x.Exec(acmeCtx(t), command.Command{
		Entity: "widget", Op: command.OpCreate, Payload: &hookModel{Kind: "bad"},
	})
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("validate hook must reject, got %v", err)
	}
}

func TestExec_ValidateHookAllows(t *testing.T) {
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "widget", Kind: registry.KindAggregate, Model: (*hookModel)(nil), GraphNode: "Widget",
		Validate: func(vals map[string]any) error {
			if vals["kind"] == "bad" {
				return errors.New("kind not allowed")
			}
			return nil
		},
	})
	x, err := command.NewExecutor(r, newFakeStore())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.Exec(acmeCtx(t), command.Command{
		Entity: "widget", Op: command.OpCreate, Payload: &hookModel{Kind: "good"},
	}); err != nil {
		t.Fatalf("valid payload must pass: %v", err)
	}
}
