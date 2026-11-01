package forgeext

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
)

func TestAnalyticsHealthRequiresSource(t *testing.T) {
	ext := &Extension{fab: &fabriq.Fabriq{}, stores: &fabriq.Stores{}}
	if err := ext.Health(context.Background()); err == nil {
		t.Fatal("stores without a source reported healthy")
	}
	if err := ext.Ready(context.Background()); err == nil {
		t.Fatal("stores without a source reported ready")
	}
}
