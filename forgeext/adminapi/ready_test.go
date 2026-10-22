package adminapi

import (
	"context"
	"testing"
)

func TestExtensionReady_TracksStart(t *testing.T) {
	e := &Extension{}
	if err := e.Ready(context.Background()); err == nil {
		t.Fatal("Ready before Start returned nil")
	}
	e.MarkStarted()
	if err := e.Ready(context.Background()); err != nil {
		t.Fatalf("Ready after Start = %v, want nil", err)
	}
	e.MarkStopped()
	if err := e.Ready(context.Background()); err == nil {
		t.Fatal("Ready after Stop returned nil")
	}
}
